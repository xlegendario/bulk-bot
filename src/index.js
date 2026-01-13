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
  AIRTABLE_COMMITMENTS_TABLE = "Commitments",

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
const commitmentsTable = base(AIRTABLE_COMMITMENTS_TABLE);

console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);
console.log("✅ Buyers table name:", AIRTABLE_BUYERS_TABLE);
console.log("✅ Opportunities table name:", AIRTABLE_OPPS_TABLE);
console.log("✅ Commitments table name:", AIRTABLE_COMMITMENTS_TABLE);

/* =========================
   Field name constants (edit here if your Airtable fields differ)
========================= */

const F = {
  // Buyers table
  BUYER_DISCORD_ID: "Discord User ID",
  BUYER_DISCORD_USERNAME: "Discord Username",

  // Opportunities table
  OPP_PRODUCT_NAME: "Product Name",
  OPP_SKU_SOFT: "SKU (Soft)",
  OPP_SKU: "SKU",
  OPP_MIN_SIZE: "Min Size",
  OPP_MAX_SIZE: "Max Size",
  OPP_CURRENCY: "Currency",
  OPP_CURRENT_SELL_PRICE: "Current Sell Price",
  OPP_START_SELL_PRICE: "Start Sell Price",
  OPP_CURRENT_DISCOUNT: "Current Discount %",
  OPP_CURRENT_TOTAL_PAIRS: "Current Total Pairs",
  OPP_NEXT_MIN_PAIRS: "Next Tier Min Pairs",
  OPP_NEXT_DISCOUNT: "Next Tier Discount %",
  OPP_PICTURE: "Picture",

  // Commitments table
  COM_OPPORTUNITY: "Opportunity",
  COM_BUYER: "Buyer",
  COM_STATUS: "Status",
  COM_DISCORD_USER_ID: "Discord User ID",
  COM_DISCORD_USER_TAG: "Discord User Tag",
  COM_DM_CHANNEL_ID: "Discord Private Channel ID",
  COM_DM_MESSAGE_ID: "Discord Summary Message ID",
  COM_LAST_ACTIVITY: "Last Activity At",
};

/* =========================
   Helpers (formatting)
========================= */

function currencySymbol(code) {
  const c = String(code || "").toUpperCase();
  if (c === "EUR") return "€";
  if (c === "USD") return "$";
  if (c === "GBP") return "£";
  return c ? `${c} ` : "";
}

function asText(v) {
  if (v === undefined || v === null) return "";
  if (Array.isArray(v)) return v.filter(Boolean).join(", ");
  return String(v);
}

function formatMoney(code, value) {
  const raw = asText(value);
  if (!raw) return "—";
  const num = Number(raw);
  if (Number.isNaN(num)) return "—";
  const sym = currencySymbol(code);
  const formatted = num % 1 === 0 ? num.toFixed(0) : num.toFixed(2);
  return `${sym}${formatted}`;
}

function formatPercent(v) {
  const raw = asText(v);
  if (!raw) return "—";
  const num = Number(raw);
  if (Number.isNaN(num)) return "—";
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
  const productName = asText(fields[F.OPP_PRODUCT_NAME]) || "Bulk Opportunity";
  const sku = asText(fields[F.OPP_SKU_SOFT]) || asText(fields[F.OPP_SKU]) || "—";
  const minSize = asText(fields[F.OPP_MIN_SIZE]) || "—";
  const maxSize = asText(fields[F.OPP_MAX_SIZE]) || "—";
  const currency = asText(fields[F.OPP_CURRENCY]) || "EUR";

  // Fallback: if Current Sell Price empty, show Start Sell Price
  const currentPrice = formatMoney(
    currency,
    fields[F.OPP_CURRENT_SELL_PRICE] ?? fields[F.OPP_START_SELL_PRICE]
  );

  const currentDiscount = formatPercent(fields[F.OPP_CURRENT_DISCOUNT] ?? 0);
  const currentTotalPairs = asText(fields[F.OPP_CURRENT_TOTAL_PAIRS]) || "—";
  const nextMinPairs = asText(fields[F.OPP_NEXT_MIN_PAIRS]) || "—";
  const nextDiscount = formatPercent(fields[F.OPP_NEXT_DISCOUNT]);

  const picUrl = getAirtableAttachmentUrl(fields[F.OPP_PICTURE]);

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

  if (picUrl) embed.setThumbnail(picUrl);

  return embed;
}

/* =========================
   Airtable: Buyer + Commitment helpers
========================= */

const escapeForFormula = (str) => String(str).replace(/'/g, "\\'");

async function upsertBuyer(discordUser) {
  const discordId = discordUser.id;
  const username = discordUser.username;

  // Find by Discord ID
  const existing = await buyersTable
    .select({
      maxRecords: 1,
      filterByFormula: `{${F.BUYER_DISCORD_ID}} = '${escapeForFormula(discordId)}'`,
    })
    .firstPage();

  if (existing.length > 0) return existing[0];

  // Create minimal buyer record
  const created = await buyersTable.create({
    [F.BUYER_DISCORD_ID]: discordId,
    [F.BUYER_DISCORD_USERNAME]: username,
  });

  return created;
}

async function findCommitment(buyerRecordId, oppRecordId) {
  // Best effort: find by linked Buyer + linked Opportunity
  // This relies on Airtable formula comparing linked record IDs using RECORD_ID() not possible directly,
  // so we use a filter on the linked fields' primary values might not be reliable.
  // Therefore: try a robust “contains recordId” search by storing the Discord User ID too.
  //
  // We will filter by Discord User ID + Opportunity contains the Opportunity ID display if present.
  // If this fails, we create a new commitment (still fine for Step 1).
  try {
    const opp = await oppsTable.find(oppRecordId);
    const oppIdDisplay = asText(opp.fields["Opportunity ID"]) || "";

    const formulaParts = [];
    formulaParts.push(`{${F.COM_DISCORD_USER_ID}} = '${escapeForFormula(String(opp._rawJson?.fields?.[F.COM_DISCORD_USER_ID] || ""))}'`);

    // ^ Above line is not usable (no opp field). We'll do a simpler search below using buyerRecordId in linked field value.
    // But Airtable formula can't match linked record IDs directly. So we skip a strict search.
  } catch (e) {
    // ignore
  }

  // For v1 Step 1: return null so we create/overwrite safely.
  return null;
}

async function createCommitment({
  oppRecordId,
  buyerRecordId,
  discordId,
  discordTag,
}) {
  const nowIso = new Date().toISOString();

  const created = await commitmentsTable.create({
    [F.COM_OPPORTUNITY]: [oppRecordId],
    [F.COM_BUYER]: [buyerRecordId],
    [F.COM_STATUS]: "Draft",
    [F.COM_DISCORD_USER_ID]: discordId,
    [F.COM_DISCORD_USER_TAG]: discordTag,
    [F.COM_LAST_ACTIVITY]: nowIso,
  });

  return created;
}

async function updateCommitmentDM(commitmentRecordId, dmChannelId, dmMessageId) {
  const nowIso = new Date().toISOString();
  await commitmentsTable.update(commitmentRecordId, {
    [F.COM_DM_CHANNEL_ID]: String(dmChannelId),
    [F.COM_DM_MESSAGE_ID]: String(dmMessageId),
    [F.COM_LAST_ACTIVITY]: nowIso,
  });
}

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

client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isButton()) return;

  const inGuild = !!interaction.guildId;
  const ephemeral = inGuild;

  // JOIN BULK button
  if (interaction.customId.startsWith("opp_join:")) {
    const opportunityRecordId = interaction.customId.split("opp_join:")[1];

    await interaction.deferReply({ ephemeral });

    try {
      // 1) Fetch opportunity
      const opp = await oppsTable.find(opportunityRecordId);
      const oppFields = opp.fields || {};

      // 2) Upsert buyer
      const buyer = await upsertBuyer(interaction.user);

      // 3) Create commitment (Draft) — for Step 1 we always create; we’ll dedupe next step
      const commitment = await createCommitment({
        oppRecordId: opportunityRecordId,
        buyerRecordId: buyer.id,
        discordId: interaction.user.id,
        discordTag: interaction.user.tag,
      });

      // 4) DM panel
      const dm = await interaction.user.createDM();

      const cartEmbed = new EmbedBuilder()
        .setTitle("🧾 Bulk Cart")
        .setDescription(
          [
            "Your cart has been created.",
            "",
            "Next step: you’ll be able to select sizes and quantities here.",
            "",
            "For now this confirms the DM flow works ✅",
          ].join("\n")
        )
        .setColor(0xffd300);

      const oppEmbed = buildOpportunityEmbed(oppFields);

      const msg = await dm.send({
        embeds: [oppEmbed, cartEmbed],
      });

      // 5) Store DM message IDs on the commitment
      await updateCommitmentDM(commitment.id, dm.id, msg.id);

      await interaction.editReply({
        content: "✅ I’ve sent you a DM to build your cart.",
      });
      return;
    } catch (err) {
      console.error("opp_join handler error:", err);

      // Common case: DMs disabled
      await interaction.editReply({
        content:
          "⚠️ I couldn’t DM you. Please enable DMs for this server (Privacy Settings) and try again.",
      });
      return;
    }
  }

  // ping test
  if (interaction.customId === "ping_test") {
    await interaction.reply({ content: "pong ✅", ephemeral: true });
  }
});

/* =========================
   Express (existing posting + syncing)
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
