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

/* =========================
   ENV
========================= */

const {
  DISCORD_TOKEN,
  AIRTABLE_API_KEY,
  AIRTABLE_BASE_ID,

  // Airtable tables
  AIRTABLE_BUYERS_TABLE = "Buyers",
  AIRTABLE_OPPS_TABLE = "Opportunities",

  // Discord channel where public opportunities are posted
  BULK_PUBLIC_CHANNEL_ID,

  // Shared secret for /post-opportunity and /sync-opportunity
  POST_OPP_SECRET,
} = process.env;

// Safe port parsing for Render
const LISTEN_PORT = Number.parseInt(process.env.PORT, 10) || 10000;

if (!DISCORD_TOKEN) {
  console.error("❌ DISCORD_TOKEN missing");
  process.exit(1);
}
if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID) {
  console.error("❌ AIRTABLE_API_KEY or AIRTABLE_BASE_ID missing");
  process.exit(1);
}

/* =========================
   Airtable
========================= */

const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);
const buyersTable = base(AIRTABLE_BUYERS_TABLE);
const oppsTable = base(AIRTABLE_OPPS_TABLE);

console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);
console.log("✅ Buyers table name:", AIRTABLE_BUYERS_TABLE);
console.log("✅ Opportunities table name:", AIRTABLE_OPPS_TABLE);

/* =========================
   Discord Client
========================= */

const client = new Client({
  intents: [GatewayIntentBits.Guilds],
  partials: [Partials.Channel],
});

client.once(Events.ClientReady, async (c) => {
  console.log(`🤖 Logged in as ${c.user.tag}`);
});

// We'll add Join Bulk handler next step
client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isButton()) return;

  if (interaction.customId === "ping_test") {
    await interaction.reply({ content: "pong ✅", ephemeral: true });
  }
});

/* =========================
   Helpers
========================= */

function currencySymbol(code) {
  const c = String(code || "").toUpperCase();
  if (c === "EUR") return "€";
  if (c === "USD") return "$";
  if (c === "GBP") return "£";
  return c ? `${c} ` : "";
}

function formatMoney(code, value) {
  if (value === undefined || value === null || value === "") return "—";
  const num = Number(value);
  if (Number.isNaN(num)) return "—";
  const sym = currencySymbol(code);
  const formatted = num % 1 === 0 ? num.toFixed(0) : num.toFixed(2);
  return `${sym}${formatted}`;
}

function formatPercent(v) {
  if (v === undefined || v === null || v === "") return "—";
  const num = Number(v);
  if (Number.isNaN(num)) return "—";
  // Airtable percent fields often return 0.04 for 4%
  const pct = (num * 100).toFixed(2).replace(/\.00$/, "");
  return `${pct}%`;
}

function getAirtableAttachmentUrl(fieldValue) {
  if (Array.isArray(fieldValue) && fieldValue.length > 0 && fieldValue[0]?.url) {
    return fieldValue[0].url;
  }
  if (typeof fieldValue === "string" && fieldValue.startsWith("http")) {
    return fieldValue;
  }
  return null;
}

function buildOpportunityEmbed(fields) {
  const productName = fields["Product Name"] || "Bulk Opportunity";
  const sku = fields["SKU (Soft)"] || fields["SKU"] || "—";
  const minSize = fields["Min Size"] || "—";
  const maxSize = fields["Max Size"] || "—";
  const currency = fields["Currency"] || "EUR";

  const currentPrice = formatMoney(currency, fields["Current Sell Price"]);
  const currentDiscount = formatPercent(fields["Current Discount %"]);
  const currentTotalPairs =
    fields["Current Total Pairs"] === undefined || fields["Current Total Pairs"] === null
      ? "—"
      : String(fields["Current Total Pairs"]);

  const nextMinPairs =
    fields["Next Tier Min Pairs"] === undefined || fields["Next Tier Min Pairs"] === null
      ? "—"
      : String(fields["Next Tier Min Pairs"]);

  const nextDiscount = formatPercent(fields["Next Tier Discount %"]);

  const picUrl = getAirtableAttachmentUrl(fields["Picture"]);

  const desc = [
    `**SKU:** \`${sku}\``,
    `**Size Range:** \`${minSize} → ${maxSize}\``,
    `**Current Price:** **${currentPrice}**`,
    `**Current Discount:** **${currentDiscount}**`,
    `**Current Total Pairs:** **${currentTotalPairs}**`,
    "",
    `**MOQ for Next Tier:** **${nextMinPairs}**`,
    `**Next Tier Discount:** **${nextDiscount}**`,
  ].join("\n");

  const embed = new EmbedBuilder()
    .setTitle(productName)
    .setDescription(desc)
    .setFooter({ text: "Join with any quantity • Price locks when bulk closes" })
    .setColor(0xffd300);

  // Small image in top-right corner
  if (picUrl) embed.setThumbnail(picUrl);

  return embed;
}

/* =========================
   Express
========================= */

const app = express();
app.use(morgan("tiny"));
app.use(express.json());

app.get("/", async (req, res) => {
  res.send("Bulk bot is live ✅");
});

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
 * Body: { opportunityRecordId: "recXXXX" }
 * Header: x-post-secret: <POST_OPP_SECRET>
 */
app.post("/post-opportunity", async (req, res) => {
  try {
    const incomingSecret = req.header("x-post-secret") || "";
    if (!POST_OPP_SECRET || incomingSecret !== POST_OPP_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    if (!BULK_PUBLIC_CHANNEL_ID) {
      return res.status(500).json({ ok: false, error: "BULK_PUBLIC_CHANNEL_ID not set" });
    }

    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) {
      return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });
    }

    const opp = await oppsTable.find(opportunityRecordId);
    const fields = opp.fields || {};

    // Prevent double posting
    if (fields["Discord Public Message ID"]) {
      return res.json({
        ok: true,
        skipped: true,
        reason: "Already posted",
        messageId: fields["Discord Public Message ID"],
      });
    }

    const embed = buildOpportunityEmbed(fields);

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`opp_join:${opportunityRecordId}`)
        .setLabel("Join Bulk")
        .setStyle(ButtonStyle.Success)
    );

    const channel = await client.channels.fetch(BULK_PUBLIC_CHANNEL_ID);
    if (!channel || !channel.isTextBased()) {
      return res.status(500).json({ ok: false, error: "Channel not found or not text-based" });
    }

    const msg = await channel.send({ embeds: [embed], components: [row] });

    // Write back to Airtable
    const updatePayload = {
      "Discord Public Channel ID": String(BULK_PUBLIC_CHANNEL_ID),
      "Discord Public Message ID": String(msg.id),
      "Post Now": false,
    };

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

/**
 * POST /sync-opportunity
 * Body: { opportunityRecordId: "recXXXX" }
 * Header: x-post-secret: <POST_OPP_SECRET>
 */
app.post("/sync-opportunity", async (req, res) => {
  try {
    const incomingSecret = req.header("x-post-secret") || "";
    if (!POST_OPP_SECRET || incomingSecret !== POST_OPP_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) {
      return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });
    }

    const opp = await oppsTable.find(opportunityRecordId);
    const fields = opp.fields || {};

    const channelId = fields["Discord Public Channel ID"];
    const messageId = fields["Discord Public Message ID"];

    if (!channelId || !messageId) {
      return res.status(400).json({
        ok: false,
        error: "Missing Discord Public Channel ID or Discord Public Message ID",
      });
    }

    const channel = await client.channels.fetch(String(channelId));
    if (!channel || !channel.isTextBased()) {
      return res.status(500).json({ ok: false, error: "Channel not found or not text-based" });
    }

    const message = await channel.messages.fetch(String(messageId));
    if (!message) {
      return res.status(500).json({ ok: false, error: "Message not found" });
    }

    const embed = buildOpportunityEmbed(fields);

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`opp_join:${opportunityRecordId}`)
        .setLabel("Join Bulk")
        .setStyle(ButtonStyle.Success)
    );

    await message.edit({ embeds: [embed], components: [row] });

    return res.json({ ok: true, synced: true });
  } catch (err) {
    console.error("sync-opportunity error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.listen(LISTEN_PORT, () => console.log(`🌐 Listening on ${LISTEN_PORT}`));

/* =========================
   Start Bot
========================= */

client.login(DISCORD_TOKEN);
