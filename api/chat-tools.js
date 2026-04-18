const { getChatToolCatalog } = require("../lib/chat-service");

module.exports = async function handler(_req, res) {
  res.setHeader(
    "Cache-Control",
    "public, max-age=300, s-maxage=300, stale-while-revalidate=600"
  );
  return res.status(200).json(getChatToolCatalog());
};
