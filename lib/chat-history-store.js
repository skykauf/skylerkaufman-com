const crypto = require("crypto");
const { Pool } = require("pg");
const { resolveDatabaseUrl } = require("./resolve-database-url");

let pool;
let poolForUrl;
let schemaReady = false;

function normalizeUrlForNodePg(url) {
  if (!url || typeof url !== "string") return url;
  let raw = url.trim();
  if (raw.startsWith("postgres://")) {
    raw = `postgresql://${raw.slice("postgres://".length)}`;
  }
  return raw;
}

function sslConfigForUrl(url) {
  if (process.env.PGSSLMODE === "disable") return false;
  const host = (() => {
    try {
      return new URL(url).hostname || "";
    } catch {
      return "";
    }
  })();
  if (host === "localhost" || host === "127.0.0.1") return false;
  return {
    rejectUnauthorized: false,
    checkServerIdentity: () => undefined,
  };
}

function getPool() {
  const url = normalizeUrlForNodePg(resolveDatabaseUrl());
  if (!url) return null;
  if (!pool || poolForUrl !== url) {
    poolForUrl = url;
    pool = new Pool({
      connectionString: url,
      max: 3,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: 5000,
      ssl: sslConfigForUrl(url),
    });
    schemaReady = false;
  }
  return pool;
}

async function ensureSchema(db) {
  if (schemaReady) return;
  await db.query(`
    CREATE SCHEMA IF NOT EXISTS app_private;
    CREATE TABLE IF NOT EXISTS app_private.chat_conversations (
      id uuid PRIMARY KEY,
      user_id uuid NOT NULL,
      title text NOT NULL DEFAULT 'New chat',
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now(),
      last_message_at timestamptz NOT NULL DEFAULT now(),
      deleted_at timestamptz
    );
    CREATE INDEX IF NOT EXISTS idx_chat_conversations_user_updated
      ON app_private.chat_conversations (user_id, updated_at DESC)
      WHERE deleted_at IS NULL;

    CREATE TABLE IF NOT EXISTS app_private.chat_messages (
      id uuid PRIMARY KEY,
      conversation_id uuid NOT NULL REFERENCES app_private.chat_conversations(id) ON DELETE CASCADE,
      user_id uuid NOT NULL,
      role text NOT NULL CHECK (role IN ('user','assistant')),
      content text NOT NULL,
      meta_json jsonb,
      created_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_chat_messages_conversation_created
      ON app_private.chat_messages (conversation_id, created_at ASC);
  `);
  schemaReady = true;
}

function newId() {
  return crypto.randomUUID();
}

function inferConversationTitle(message) {
  const s = String(message || "").replace(/\s+/g, " ").trim();
  if (!s) return "New chat";
  return s.slice(0, 80);
}

async function ensureConversationForUser({ userId, conversationId, titleHint }) {
  const db = getPool();
  if (!db) return { error: "DATABASE_URL is not set." };
  await ensureSchema(db);
  if (conversationId) {
    const owned = await db.query(
      `
      SELECT id, user_id, title, created_at, updated_at, last_message_at
      FROM app_private.chat_conversations
      WHERE id = $1::uuid AND user_id = $2::uuid AND deleted_at IS NULL
      LIMIT 1
      `,
      [conversationId, userId]
    );
    if (owned.rows[0]) return { conversation: owned.rows[0], created: false };
  }
  const id = newId();
  const title = inferConversationTitle(titleHint);
  const inserted = await db.query(
    `
    INSERT INTO app_private.chat_conversations (id, user_id, title)
    VALUES ($1::uuid, $2::uuid, $3::text)
    RETURNING id, user_id, title, created_at, updated_at, last_message_at
    `,
    [id, userId, title]
  );
  return { conversation: inserted.rows[0], created: true };
}

async function appendConversationMessages({ userId, conversationId, userMessage, assistantMessage, assistantMeta }) {
  const db = getPool();
  if (!db) return { error: "DATABASE_URL is not set." };
  await ensureSchema(db);

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const owned = await client.query(
      `
      SELECT id FROM app_private.chat_conversations
      WHERE id = $1::uuid AND user_id = $2::uuid AND deleted_at IS NULL
      LIMIT 1
      `,
      [conversationId, userId]
    );
    if (!owned.rows[0]) {
      await client.query("ROLLBACK");
      return { error: "Conversation not found." };
    }
    if (userMessage && String(userMessage).trim()) {
      await client.query(
        `
        INSERT INTO app_private.chat_messages (id, conversation_id, user_id, role, content)
        VALUES ($1::uuid, $2::uuid, $3::uuid, 'user', $4::text)
        `,
        [newId(), conversationId, userId, String(userMessage).slice(0, 12000)]
      );
    }
    await client.query(
      `
      INSERT INTO app_private.chat_messages (id, conversation_id, user_id, role, content, meta_json)
      VALUES ($1::uuid, $2::uuid, $3::uuid, 'assistant', $4::text, $5::jsonb)
      `,
      [
        newId(),
        conversationId,
        userId,
        String(assistantMessage || "").slice(0, 12000),
        assistantMeta && typeof assistantMeta === "object" ? assistantMeta : null,
      ]
    );
    await client.query(
      `
      UPDATE app_private.chat_conversations
      SET updated_at = now(), last_message_at = now()
      WHERE id = $1::uuid
      `,
      [conversationId]
    );
    await client.query("COMMIT");
    return { ok: true };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {
      /* ignore */
    }
    return { error: err.message || String(err) };
  } finally {
    client.release();
  }
}

async function listConversationsForUser(userId) {
  const db = getPool();
  if (!db) return { error: "DATABASE_URL is not set." };
  await ensureSchema(db);
  const result = await db.query(
    `
    SELECT
      c.id,
      c.title,
      c.created_at,
      c.updated_at,
      c.last_message_at,
      (
        SELECT m.content
        FROM app_private.chat_messages m
        WHERE m.conversation_id = c.id
        ORDER BY m.created_at DESC
        LIMIT 1
      ) AS last_message_preview
    FROM app_private.chat_conversations c
    WHERE c.user_id = $1::uuid
      AND c.deleted_at IS NULL
    ORDER BY c.updated_at DESC
    LIMIT 100
    `,
    [userId]
  );
  return { conversations: result.rows };
}

async function getConversationForUser({ userId, conversationId }) {
  const db = getPool();
  if (!db) return { error: "DATABASE_URL is not set." };
  await ensureSchema(db);

  const c = await db.query(
    `
    SELECT id, title, created_at, updated_at, last_message_at
    FROM app_private.chat_conversations
    WHERE id = $1::uuid AND user_id = $2::uuid AND deleted_at IS NULL
    LIMIT 1
    `,
    [conversationId, userId]
  );
  if (!c.rows[0]) return { error: "Conversation not found." };

  const msgs = await db.query(
    `
    SELECT id, role, content, meta_json, created_at
    FROM app_private.chat_messages
    WHERE conversation_id = $1::uuid
    ORDER BY created_at ASC
    LIMIT 500
    `,
    [conversationId]
  );
  return { conversation: c.rows[0], messages: msgs.rows };
}

async function deleteConversationForUser({ userId, conversationId }) {
  const db = getPool();
  if (!db) return { error: "DATABASE_URL is not set." };
  await ensureSchema(db);
  const result = await db.query(
    `
    UPDATE app_private.chat_conversations
    SET deleted_at = now(), updated_at = now()
    WHERE id = $1::uuid AND user_id = $2::uuid AND deleted_at IS NULL
    RETURNING id
    `,
    [conversationId, userId]
  );
  if (!result.rows[0]) return { error: "Conversation not found." };
  return { ok: true };
}

module.exports = {
  ensureConversationForUser,
  appendConversationMessages,
  listConversationsForUser,
  getConversationForUser,
  deleteConversationForUser,
};
