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
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
} from "discord.js";

/* =========================
   ENV
========================= */

const {
  DISCORD_TOKEN,
  AIRTABLE_API_KEY,
  AIRTABLE_BASE_ID,

  AIRTABLE_BUYERS_TABLE = "Buyers",
  AIRTABLE_OPPS_TABLE = "Opportunities",
  AIRTABLE_COMMITMENTS_TABLE = "Commitments",
  AIRTABLE_LINES_TABLE = "Commitment Lines",

  BULK_PUBLIC_CHANNEL_ID,
  POST_OPP_SECRET,
} = process.env;

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
const linesTable = base(AIRTABLE_LINES_TABLE);

console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);

/* =========================
   Field name constants
========================= */

const F = {
  // Buyers
  BUYER_DISCORD_ID: "Discord User ID",
  BUYER_DISCORD_USERNAME: "Discord Username",

  // Opportunities
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

  // IMPORTANT: create this as lookup text in Opportunities from Size Presets
  OPP_SIZE_LADDER: "Size Ladder",

  // Commitments
  COM_OPPORTUNITY: "Opportunity",
  COM_OPP_RECORD_ID: "Opportunity Record ID",
  COM_BUYER: "Buyer",
  COM_STATUS: "Status",
  COM_DISCORD_USER_ID: "Discord User ID",
  COM_DISCORD_USER_TAG: "Discord User Tag",
  COM_DM_CHANNEL_ID: "Discord Private Channel ID",
  COM_DM_MESSAGE_ID: "Discord Summary Message ID",
  COM_LAST_ACTIVITY: "Last Activity At",

  // Lines
  LINE_COMMITMENT: "Commitment",
  LINE_COMMITMENT_RECORD_ID: "Commitment Record ID",
  LINE_SIZE: "Size",
  LINE_QTY: "Quantity",
};

/* =========================
   Helpers
========================= */

function asText(v) {
  if (v === undefined || v === null) return "";
  if (Array.isArray(v)) return v.filter(Boolean).join(", ");
  return String(v);
}

function currencySymbol(code) {
  const c = String(code || "").toUpperCase();
  if (c === "EUR") return "€";
  if (c === "USD") return "$";
  if (c === "GBP") return "£";
  return c ? `${c} ` : "";
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

function encodeSize(size) {
  return encodeURIComponent(size);
}
function decodeSize(encoded) {
  return decodeURIComponent(encoded);
}

function parseLadder(ladderText) {
  // ladderText can be "35,36,36.5,..." or "36,36 2/3,37 1/3,..."
  const raw = asText(ladderText);
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function sliceSizes(ladderArr, minSize, maxSize) {
  const min = asText(minSize).trim();
  const max = asText(maxSize).trim();
  const iMin = ladderArr.indexOf(min);
  const iMax = ladderArr.indexOf(max);
  if (iMin === -1 || iMax === -1) {
    throw new Error(`Min/Max size not found in ladder. Min=${min} Max=${max}`);
  }
  if (iMin > iMax) {
    throw new Error(`Min size is greater than Max size. Min=${min} Max=${max}`);
  }
  return ladderArr.slice(iMin, iMax + 1);
}

/* =========================
   Embed builders
========================= */

function buildOpportunityEmbed(fields) {
  const productName = asText(fields[F.OPP_PRODUCT_NAME]) || "Bulk Opportunity";
  const sku = asText(fields[F.OPP_SKU_SOFT]) || asText(fields[F.OPP_SKU]) || "—";
  const minSize = asText(fields[F.OPP_MIN_SIZE]) || "—";
  const maxSize = asText(fields[F.OPP_MAX_SIZE]) || "—";
  const currency = asText(fields[F.OPP_CURRENCY]) || "EUR";

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
    .setTitle(productName.length > 256 ? productName.slice(0, 253) + "..." : productName)
    .setDescription(desc)
    .setFooter({ text: "Join with any quantity • Price locks when bulk closes" })
    .setColor(0xffd300);

  if (picUrl) embed.setThumbnail(picUrl);
  return embed;
}

async function buildCartEmbed(commitment, lines, allowedSizes) {
  const status = asText(commitment.fields[F.COM_STATUS]) || "Draft";

  // sort lines by allowedSizes order if possible
  const order = new Map(allowedSizes.map((s, idx) => [s, idx]));
  const sorted = [...lines].sort((a, b) => {
    const sa = asText(a.fields[F.LINE_SIZE]);
    const sb = asText(b.fields[F.LINE_SIZE]);
    return (order.get(sa) ?? 9999) - (order.get(sb) ?? 9999);
  });

  const rows = [];
  let total = 0;

  for (const r of sorted) {
    const size = asText(r.fields[F.LINE_SIZE]);
    const qty = Number(r.fields[F.LINE_QTY] ?? 0);
    if (!qty) continue;
    total += qty;
    rows.push(`• **EU ${size}** × **${qty}**`);
  }

  const desc = rows.length
    ? rows.join("\n")
    : "_No sizes added yet. Tap a size below to add quantity._";

  return new EmbedBuilder()
    .setTitle("🧾 Bulk Cart")
    .setDescription(
      [
        `**Status:** ${status}`,
        `**Total pairs:** **${total}**`,
        "",
        desc,
      ].join("\n")
    )
    .setColor(0xffd300);
}

/* =========================
   Airtable helpers
========================= */

const escapeForFormula = (str) => String(str).replace(/'/g, "\\'");

async function upsertBuyer(discordUser) {
  const discordId = discordUser.id;
  const username = discordUser.username;

  const existing = await buyersTable
    .select({
      maxRecords: 1,
      filterByFormula: `{${F.BUYER_DISCORD_ID}} = '${escapeForFormula(discordId)}'`,
    })
    .firstPage();

  if (existing.length > 0) return existing[0];

  return await buyersTable.create({
    [F.BUYER_DISCORD_ID]: discordId,
    [F.BUYER_DISCORD_USERNAME]: username,
  });
}

async function findOrCreateCommitment({ oppRecordId, buyerRecordId, discordUser }) {
  // find latest commitment for this buyer+opp where not locked/cancelled
  const records = await commitmentsTable
    .select({
      maxRecords: 1,
      filterByFormula: `AND(
        {${F.COM_OPP_RECORD_ID}} = '${escapeForFormula(oppRecordId)}',
        {${F.COM_DISCORD_USER_ID}} = '${escapeForFormula(discordUser.id)}',
        OR(
          {${F.COM_STATUS}} = 'Draft',
          {${F.COM_STATUS}} = 'Submitted'
        )
      )`,
      sort: [{ field: F.COM_LAST_ACTIVITY, direction: "desc" }],
    })
    .firstPage();

  if (records.length > 0) return records[0];

  const nowIso = new Date().toISOString();
  return await commitmentsTable.create({
    [F.COM_OPPORTUNITY]: [oppRecordId],
    [F.COM_OPP_RECORD_ID]: oppRecordId,
    [F.COM_BUYER]: [buyerRecordId],
    [F.COM_STATUS]: "Draft",
    [F.COM_DISCORD_USER_ID]: discordUser.id,
    [F.COM_DISCORD_USER_TAG]: discordUser.tag,
    [F.COM_LAST_ACTIVITY]: nowIso,
  });
}

async function upsertLine({ commitmentId, size, qty }) {
  // find existing line for this commitment + size
  const rows = await linesTable
    .select({
      maxRecords: 1,
      filterByFormula: `AND(
        {${F.LINE_COMMITMENT_RECORD_ID}} = '${escapeForFormula(commitmentId)}',
        {${F.LINE_SIZE}} = '${escapeForFormula(size)}'
      )`,
    })
    .firstPage();

  if (qty <= 0) {
    // delete if exists
    if (rows.length > 0) await linesTable.destroy(rows[0].id);
    return;
  }

  if (rows.length > 0) {
    await linesTable.update(rows[0].id, { [F.LINE_QTY]: qty });
    return;
  }

  await linesTable.create({
    [F.LINE_COMMITMENT]: [commitmentId],
    [F.LINE_COMMITMENT_RECORD_ID]: commitmentId,
    [F.LINE_SIZE]: size,
    [F.LINE_QTY]: qty,
  });
}

async function getLinesForCommitment(commitmentId) {
  return await linesTable
    .select({
      filterByFormula: `{${F.LINE_COMMITMENT_RECORD_ID}} = '${escapeForFormula(commitmentId)}'`,
      maxRecords: 200,
    })
    .firstPage();
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
   Discord UI builders
========================= */

function buildSizeButtons(allowedSizes, oppRecordId) {
  // max 25 buttons total per message; sneaker runs are usually <= 20
  const buttons = allowedSizes.slice(0, 25).map((s) =>
    new ButtonBuilder()
      .setCustomId(`size_pick:${oppRecordId}:${encodeSize(s)}`)
      .setLabel(`EU ${s}`)
      .setStyle(ButtonStyle.Secondary)
  );

  // rows of 5
  const rows = [];
  for (let i = 0; i < buttons.length; i += 5) {
    rows.push(new ActionRowBuilder().addComponents(buttons.slice(i, i + 5)));
  }
  return rows;
}

function buildCartActionRow(opp
