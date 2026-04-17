module.exports = async function handler(_req, res) {
  const url = (process.env.SUPABASE_URL || "").trim().replace(/\/$/, "");
  const anonKey = (process.env.SUPABASE_ANON_KEY || "").trim();
  const enabled = !!(url && anonKey);
  res.setHeader(
    "Cache-Control",
    "public, max-age=120, s-maxage=120, stale-while-revalidate=300"
  );
  return res.status(200).json({
    enabled,
    url: enabled ? url : "",
    anonKey: enabled ? anonKey : "",
  });
};
