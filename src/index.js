import "dotenv/config";
import express from "express";
import morgan from "morgan";
import Airtable from "airtable";
import { Client, GatewayIntentBits, Partials, Events } from "discord.js";

/**
 * ENV VARS REQUIRED ON RENDER
 */
const {
  DISCORD_TOKEN,
  AIRTABLE_API_KEY,
  AIRTABLE_BASE_ID,

  // Table names (set exactly to your Airtable tables)
  AIRTABLE_BUYERS_TABLE = "Buyers",
  AIRTABLE_OPPS_TABLE = "Opportunities",
  AIRTABLE_COMMITMENTS_TABLE = "Commitments",
  AIRTABLE_LINES_TABLE = "Commitment Lines",
  AIRTABLE_SKU_TABLE = "SKU Master",
  AIRTABLE_SIZEPRESETS_TABLE = "Size Presets",
  AIRTABLE_TIERRULES_TABLE = "Tier Rules",
  AIRTABLE_TIERSETS_TABLE = "Tier Rule Sets",

  PORT = 10000
} = process.env;

if (!DISCORD_TOKEN) {
  console.error("❌ DISCORD_TOKEN missing");
  process.exit(1);
}
if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID) {
  console.error("❌ AIRTABLE_API_KEY or AIRTABLE_BASE_ID missing");
  process.exit(1);
}

/**
 * Airtable
 */
const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);
const buyersTable = base(AIRTABLE_BUYERS_TABLE);

// quick sanity test on startup (doesn't fetch records)
console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);
console.log("✅ Buyers table name:", AIRTABLE_BUYERS_TABLE);

/**
 * Discord client
 */
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    // add more later when needed; keep minimal for v1
  ],
  partials: [Partials.Channel],
});

client.once(Events.ClientReady, async (c) => {
  console.log(`🤖 Logged in as ${c.user.tag}`);
});

/**
 * Minimal interaction placeholder (we add bulk logic next)
 */
client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isButton()) return;

  // temporary test button handler
  if (interaction.customId === "ping_test") {
    await interaction.reply({ content: "pong ✅", ephemeral: true });
  }
});

/**
 * Express healthcheck for Render
 */
const app = express();
app.use(morgan("tiny"));
app.use(express.json());

app.get("/", async (req, res) => {
  res.send("Bulk bot is live ✅");
});

// Optional: Airtable connectivity test endpoint
app.get("/airtable-test", async (req, res) => {
  try {
    // Get 1 record just to prove auth works (safe)
    const records = await buyersTable.select({ maxRecords: 1 }).firstPage();
    res.json({ ok: true, buyers_records_found: records.length });
  } catch (err) {
    console.error("Airtable test failed:", err);
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.listen(PORT, () => console.log(`🌐 Listening on ${PORT}`));

/**
 * Start bot
 */
client.login(DISCORD_TOKEN);
