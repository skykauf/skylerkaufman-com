const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim().replace(/\/$/, "");
const SUPABASE_ANON_KEY = (process.env.SUPABASE_ANON_KEY || "").trim();

function getSupabaseAuthConfig() {
  return {
    enabled: !!(SUPABASE_URL && SUPABASE_ANON_KEY),
    url: SUPABASE_URL,
    anonKeyPresent: !!SUPABASE_ANON_KEY,
  };
}

async function getAuthenticatedUser(req) {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    return { user: null, error: null, reason: "supabase_auth_not_configured" };
  }
  const authHeader = req?.headers?.authorization || req?.headers?.Authorization || "";
  const raw = typeof authHeader === "string" ? authHeader.trim() : "";
  if (!raw.toLowerCase().startsWith("bearer ")) {
    return { user: null, error: null, reason: "missing_bearer_token" };
  }
  const token = raw.slice("bearer ".length).trim();
  if (!token) {
    return { user: null, error: "Missing bearer token." };
  }

  try {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      method: "GET",
      headers: {
        apikey: SUPABASE_ANON_KEY,
        Authorization: `Bearer ${token}`,
      },
    });
    if (!res.ok) {
      const text = await res.text();
      return {
        user: null,
        error: `Invalid auth token (${res.status}).`,
        detail: text.slice(0, 240),
      };
    }
    const data = await res.json();
    if (!data || !data.id) {
      return { user: null, error: "Authenticated user missing id." };
    }
    return {
      user: {
        id: String(data.id),
        email: data.email ? String(data.email) : null,
      },
      error: null,
      reason: null,
    };
  } catch (err) {
    return {
      user: null,
      error: "Failed to verify auth token.",
      detail: err && err.message ? err.message : String(err),
    };
  }
}

module.exports = {
  getAuthenticatedUser,
  getSupabaseAuthConfig,
};
