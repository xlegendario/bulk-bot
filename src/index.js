import "dotenv/config";
import express from "express";
import morgan from "morgan";
import Airtable from "airtable";
import {
  Client,
  GatewayIntentBits,
  Partials,
  Events,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
} from "discord.js";

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

  // Where to post public opportunities
  BULK_PUBLIC_CHANNEL_ID,

  // Secret to protect /post-opportunity endpoint
  POST_OPP_SECRET,
} = process.env;

// Render can provide PORT as a string (or it can be blank if misconfigured).
const LISTEN_PORT = Number.parseInt(process.env.PORT, 10) || 10000;

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
const oppsTable = base(AIRTABLE_OPPS_TABLE);

// quick sanity test on startup (doesn't fetch records)
console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);
console.log("✅ Buyers table name:", AIRTABLE_BUYERS_TABLE);
console.log("✅ Opportunities table name:", AIRTABLE_OPPS_TABLE);

/**
 * Discord client
 */
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    // keep minimal for now
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
    const records = await buyersTable.select({ maxRecords: 1 }).firstPage();
    res.json({ ok: true, buyers_records_found: records.length });
  } catch (err) {
    console.error("Airtable test failed:", err);
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

/**
 * POST /post-opportunity
 * Called by Airtable Automation when "Post Now" is checked.
 * Body: { opportunityRecordId: "recXXXX" }
 * Header: x-post-secret: <POST_OPP_SECRET>
 */
app.post("/post-opportunity", async (req, res) => {
  try {
    // Security check
    const incomingSecret = req.header("x-post-secret") || "";
    if (!POST_OPP_SECRET || incomingSecret !== POST_OPP_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    if (!BULK_PUBLIC_CHANNEL_ID) {
      return res
        .status(500)
        .json({ ok: false, error: "BULK_PUBLIC_CHANNEL_ID not set" });
    }

    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) {
      return res
        .status(400)
        .json({ ok: false, error: "opportunityRecordId is required" });
    }

    // Fetch Opportunity from Airtable
    const opp = await oppsTable.find(opportunityRecordId);
    const fields = opp.fields || {};

    // Prevent double-posting
    const existingMsgId = fields["Discord Public Message ID"];
    if (existingMsgId) {
      return res.json({
        ok: true,
        skipped: true,
        reason: "Already posted",
        messageId: existingMsgId,
      });
    }

    // Minimal fields used for the first version embed
    const oppId = fields["Opportunity ID"] || opportunityRecordId;
    const productName = fields["Product Name"] || "Bulk Opportunity";
    const skuSoft = fields["SKU (Soft)"] || fields["SKU"] || "—";
    const minSize = fields["Min Size"] || "—";
    const maxSize = fields["Max Size"] || "—";
    const startSell = fields["Start Sell Price"] ?? "—";
    const currency = fields["Currency"] || "EUR";

    // Build embed
    const embed = new EmbedBuilder()
      .setTitle(`📦 ${productName}`)
      .setDescription(
        [
          `**Opportunity:** \`${oppId}\``,
          `**SKU:** \`${skuSoft}\``,
          `**Sizes:** \`${minSize} → ${maxSize}\``,
          "",
          `**Start price:** ${currency} ${startSell}`,
          "",
          "_Join with any quantity — even 1–2 pairs._",
        ].join("\n")
      )
      .setColor(0xffd300);

    // Button: Join Bulk
    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`opp_join:${opportunityRecordId}`)
        .setLabel("Join Bulk")
        .setStyle(ButtonStyle.Success)
    );

    // Post to Discord channel
    const channel = await client.channels.fetch(BULK_PUBLIC_CHANNEL_ID);
    if (!channel || !channel.isTextBased()) {
      return res
        .status(500)
        .json({ ok: false, error: "Channel not found or not text-based" });
    }

    const msg = await channel.send({ embeds: [embed], components: [row] });

    // Write back to Airtable
    const updatePayload = {
      "Discord Public Channel ID": String(BULK_PUBLIC_CHANNEL_ID),
      "Discord Public Message ID": String(msg.id),
      "Post Now": false,
    };

    // Only write Posted At if the field exists in Airtable
    if (fields["Posted At"] !== undefined) {
      updatePayload["Posted At"] = new Date().toISOString();
    }

    await oppsTable.update(opportunityRecordId, updatePayload);

    return res.json({ ok: true, posted: true, messageId: msg.id });
  } catch (err) {
    console.error("post-opportunity error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.listen(LISTEN_PORT, () => console.log(`🌐 Listening on ${LISTEN_PORT}`));

/**
 * Start bot
 */
client.login(DISCORD_TOKEN);
