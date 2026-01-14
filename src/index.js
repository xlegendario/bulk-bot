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
  MessageFlags,
} from "discord.js";

// Prevent the process from crashing on unhandled promise rejections (e.g., Discord "Unknown interaction")
process.on("unhandledRejection", (err) => {
  console.error("Unhandled promise rejection:", err);
});

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
  AIRTABLE_SIZE_PRESETS_TABLE = "Size Presets",

  // Tier engine tables
  AIRTABLE_TIER_RULES_TABLE = "Tier Rules",
  AIRTABLE_TIER_RULE_SETS_TABLE = "Tier Rule Sets",

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
const sizePresetsTable = base(AIRTABLE_SIZE_PRESETS_TABLE);
const tierRulesTable = base(AIRTABLE_TIER_RULES_TABLE);
const tierRuleSetsTable = base(AIRTABLE_TIER_RULE_SETS_TABLE);

console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);
console.log("✅ Buyers table:", AIRTABLE_BUYERS_TABLE);
console.log("✅ Opportunities table:", AIRTABLE_OPPS_TABLE);
console.log("✅ Commitments table:", AIRTABLE_COMMITMENTS_TABLE);
console.log("✅ Commitment Lines table:", AIRTABLE_LINES_TABLE);
console.log("✅ Size Presets table:", AIRTABLE_SIZE_PRESETS_TABLE);
console.log("✅ Tier Rules table:", AIRTABLE_TIER_RULES_TABLE);
console.log("✅ Tier Rule Sets table:", AIRTABLE_TIER_RULE_SETS_TABLE);

/* =========================
   Field name constants
   (edit here if your Airtable fields differ)
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
  OPP_CURRENT_DISCOUNT: "Current Discount %", // store as decimal (0.02) for 2%
  OPP_CURRENT_TOTAL_PAIRS: "Current Total Pairs",
  OPP_NEXT_MIN_PAIRS: "Next Tier Min Pairs",
  OPP_NEXT_DISCOUNT: "Next Tier Discount %", // store as decimal (0.03) for 3%
  OPP_PICTURE: "Picture",
  OPP_ALLOWED_SIZES: "Allowed Sizes (Generated)",

  // Commitments
  COM_OPPORTUNITY: "Opportunity",
  COM_BUYER: "Buyer",
  COM_STATUS: "Status",
  COM_DISCORD_USER_ID: "Discord User ID",
  COM_DISCORD_USER_TAG: "Discord User Tag",
  COM_DM_CHANNEL_ID: "Discord Private Channel ID",
  COM_DM_MESSAGE_ID: "Discord Summary Message ID",
  COM_LAST_ACTIVITY: "Last Activity At",
  COM_OPP_RECORD_ID: "Opportunity Record ID", // helper field we set
  COM_COMMITTED_AT: "Committed At", // your Airtable field name

  // Lines
  LINE_COMMITMENT: "Commitment",
  LINE_COMMITMENT_RECORD_ID: "Commitment Record ID",
  LINE_SIZE: "Size",
  LINE_QTY: "Quantity",

  // Size Presets
  PRESET_SIZE_LADDER: "Size Ladder",
  PRESET_LINKED_SKUS: "Linked SKU's",

  // Tier Rules
  TR_MIN_PAIRS: "Min Pairs",
  TR_DISCOUNT_PCT: "Discount %", // your table uses 1,2,3,... (percent)

  // Tier Rule Sets
  TRS_OPPORTUNITIES: "Opportunities", // linked to Opportunities
  TRS_TIER_RULES: "Tier Rules", // linked to Tier Rules
};

/* =========================
   Status rules
========================= */

// Buyers can edit cart only in these statuses:
const EDITABLE_STATUSES = new Set(["Draft", "Editing"]);

// Everything in this set is hard-locked (no edits, no add-more)
const HARD_LOCKED_STATUSES = new Set(["Locked", "Deposit Paid", "Paid", "Cancelled"]);

// Commitments that should COUNT towards Opportunity totals (tier progress)
// Important: Editing must still count, otherwise totals/discount would drop when someone clicks Add More
const COUNTED_STATUSES = new Set(["Submitted", "Editing", "Locked", "Deposit Paid", "Paid"]);

/* =========================
   Helpers
========================= */

const escapeForFormula = (str) => String(str).replace(/'/g, "\\'");

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
  // expects decimal (0.02) -> 2%
  const raw = asText(v);
  if (raw === "") return "—";
  const num = Number(raw);
  if (Number.isNaN(num)) return "—";
  const pct = (num * 100).toFixed(2).replace(/\.00$/, "");
  return `${pct}%`;
}

function getAirtableAttachmentUrl(fieldValue) {
  if (Array.isArray(fieldValue) && fieldValue.length > 0 && fieldValue[0]?.url) return fieldValue[0].url;
  if (typeof fieldValue === "string" && fieldValue.startsWith("http")) return fieldValue;
  return null;
}

function parseSizeList(v) {
  const raw = asText(v);
  if (!raw) return [];
  return raw.split(",").map((s) => s.trim()).filter(Boolean);
}

function parseLadder(v) {
  return asText(v)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function sliceLadderByMinMax(ladder, minRaw, maxRaw) {
  const min = String(minRaw || "").trim();
  const max = String(maxRaw || "").trim();
  if (!min && !max) return ladder;

  const i1 = min ? ladder.indexOf(min) : -1;
  const i2 = max ? ladder.indexOf(max) : -1;

  // If min/max not found, return full ladder rather than no buttons
  if ((min && i1 === -1) || (max && i2 === -1)) return ladder;

  if (min && !max) return ladder.slice(i1);
  if (!min && max) return ladder.slice(0, i2 + 1);

  const start = Math.min(i1, i2);
  const end = Math.max(i1, i2);
  return ladder.slice(start, end + 1);
}

async function getPresetLadderBySku(sku) {
  const skuKey = String(sku || "").trim().toUpperCase();
  if (!skuKey) return [];

  const rows = await sizePresetsTable
    .select({
      maxRecords: 1,
      filterByFormula: `FIND('${escapeForFormula(skuKey)}', UPPER({${F.PRESET_LINKED_SKUS}} & '')) > 0`,
    })
    .firstPage();

  if (!rows.length) return [];
  return parseLadder(rows[0].fields[F.PRESET_SIZE_LADDER]);
}

// Option B: compute allowed sizes + write back to Opportunity the first time
async function resolveAllowedSizesAndMaybeWriteback(oppRecordId, oppFields) {
  const existing = parseSizeList(oppFields[F.OPP_ALLOWED_SIZES]);
  if (existing.length) return existing;

  const sku = asText(oppFields[F.OPP_SKU_SOFT]) || asText(oppFields[F.OPP_SKU]);
  const ladder = await getPresetLadderBySku(sku);
  if (!ladder.length) return [];

  const sliced = sliceLadderByMinMax(ladder, oppFields[F.OPP_MIN_SIZE], oppFields[F.OPP_MAX_SIZE]);
  if (!sliced.length) return [];

  await oppsTable.update(oppRecordId, {
    [F.OPP_ALLOWED_SIZES]: sliced.join(", "),
  });

  return sliced;
}

function sizeKeyEncode(size) {
  return encodeURIComponent(size).replace(/%/g, "_");
}
function sizeKeyDecode(key) {
  return decodeURIComponent(key.replace(/_/g, "%"));
}

function buildOpportunityEmbed(fields) {
  const productName = asText(fields[F.OPP_PRODUCT_NAME]) || "Bulk Opportunity";
  const sku = asText(fields[F.OPP_SKU_SOFT]) || asText(fields[F.OPP_SKU]) || "—";
  const minSize = asText(fields[F.OPP_MIN_SIZE]) || "—";
  const maxSize = asText(fields[F.OPP_MAX_SIZE]) || "—";
  const currency = asText(fields[F.OPP_CURRENCY]) || "EUR";

  const currentPrice = formatMoney(currency, fields[F.OPP_CURRENT_SELL_PRICE] ?? fields[F.OPP_START_SELL_PRICE]);
  const currentDiscount = formatPercent(fields[F.OPP_CURRENT_DISCOUNT] ?? 0);
  const currentTotalPairs = asText(fields[F.OPP_CURRENT_TOTAL_PAIRS]) || "—";
  const nextMinPairs = asText(fields[F.OPP_NEXT_MIN_PAIRS]) || "—";
  const nextDiscount = formatPercent(fields[F.OPP_NEXT_DISCOUNT]) || "—";
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
  ].join(String.fromCharCode(10));

  const title = productName.length > 256 ? productName.slice(0, 253) + "..." : productName;

  const embed = new EmbedBuilder()
    .setTitle(title)
    .setDescription(desc)
    .setFooter({ text: "Join with any quantity • Price locks when bulk closes" })
    .setColor(0xffd300);

  if (picUrl) embed.setThumbnail(picUrl);
  return embed;
}

async function upsertBuyer(discordUser) {
  const discordId = discordUser.id;
  const username = discordUser.username;

  const existing = await buyersTable
    .select({
      maxRecords: 1,
      filterByFormula: `{${F.BUYER_DISCORD_ID}} = '${escapeForFormula(discordId)}'`,
    })
    .firstPage();

  if (existing.length) {
    try {
      await buyersTable.update(existing[0].id, { [F.BUYER_DISCORD_USERNAME]: username });
    } catch (_) {}
    return existing[0];
  }

  return await buyersTable.create({
    [F.BUYER_DISCORD_ID]: discordId,
    [F.BUYER_DISCORD_USERNAME]: username,
  });
}

async function createCommitment({ oppRecordId, buyerRecordId, discordId, discordTag }) {
  const nowIso = new Date().toISOString();
  const payload = {
    [F.COM_OPPORTUNITY]: [oppRecordId],
    [F.COM_BUYER]: [buyerRecordId],
    [F.COM_STATUS]: "Draft",
    [F.COM_DISCORD_USER_ID]: discordId,
    [F.COM_DISCORD_USER_TAG]: discordTag,
    [F.COM_LAST_ACTIVITY]: nowIso,
    [F.COM_OPP_RECORD_ID]: oppRecordId,
  };
  return await commitmentsTable.create(payload);
}

async function updateCommitmentDM(commitmentRecordId, dmChannelId, dmMessageId) {
  await commitmentsTable.update(commitmentRecordId, {
    [F.COM_DM_CHANNEL_ID]: String(dmChannelId),
    [F.COM_DM_MESSAGE_ID]: String(dmMessageId),
    [F.COM_LAST_ACTIVITY]: new Date().toISOString(),
  });
}

async function touchCommitment(commitmentRecordId, patch = {}) {
  await commitmentsTable.update(commitmentRecordId, {
    [F.COM_LAST_ACTIVITY]: new Date().toISOString(),
    ...patch,
  });
}

async function findLatestCommitment(discordUserId, oppRecordId) {
  const rows = await commitmentsTable
    .select({
      maxRecords: 1,
      sort: [{ field: "Created At", direction: "desc" }],
      filterByFormula: `AND(
        {${F.COM_DISCORD_USER_ID}} = '${escapeForFormula(discordUserId)}',
        {${F.COM_OPP_RECORD_ID}} = '${escapeForFormula(oppRecordId)}'
      )`,
    })
    .firstPage();
  return rows.length ? rows[0] : null;
}

async function getCommitmentStatus(commitmentRecordId) {
  const fresh = await commitmentsTable.find(commitmentRecordId);
  return asText(fresh.fields[F.COM_STATUS]) || "Draft";
}

async function upsertLine(commitmentRecordId, size, qty) {
  const found = await linesTable
    .select({
      maxRecords: 1,
      filterByFormula: `AND(
        {${F.LINE_COMMITMENT_RECORD_ID}} = '${escapeForFormula(commitmentRecordId)}',
        {${F.LINE_SIZE}} = '${escapeForFormula(size)}'
      )`,
    })
    .firstPage();

  if (qty <= 0) {
    if (found.length) await linesTable.destroy(found[0].id);
    return;
  }

  if (found.length) {
    await linesTable.update(found[0].id, { [F.LINE_QTY]: qty });
    return;
  }

  await linesTable.create({
    [F.LINE_COMMITMENT]: [commitmentRecordId],
    [F.LINE_COMMITMENT_RECORD_ID]: commitmentRecordId,
    [F.LINE_SIZE]: size,
    [F.LINE_QTY]: qty,
  });
}

async function getLineQty(commitmentRecordId, size) {
  const found = await linesTable
    .select({
      maxRecords: 1,
      filterByFormula: `AND(
        {${F.LINE_COMMITMENT_RECORD_ID}} = '${escapeForFormula(commitmentRecordId)}',
        {${F.LINE_SIZE}} = '${escapeForFormula(size)}'
      )`,
    })
    .firstPage();

  if (!found.length) return 0;
  return Number(found[0].fields[F.LINE_QTY] || 0);
}

async function getCartLinesText(commitmentRecordId) {
  const rows = await linesTable
    .select({
      filterByFormula: `{${F.LINE_COMMITMENT_RECORD_ID}} = '${escapeForFormula(commitmentRecordId)}'`,
      maxRecords: 200,
    })
    .firstPage();

  if (!rows.length) return "_No sizes selected yet._";

  const items = rows
    .map((r) => ({
      size: asText(r.fields[F.LINE_SIZE]),
      qty: Number(r.fields[F.LINE_QTY] || 0),
    }))
    .filter((x) => x.size && x.qty > 0);

  if (!items.length) return "_No sizes selected yet._";
  items.sort((a, b) => a.size.localeCompare(b.size));
  return items.map((x) => `• **${x.size}** × **${x.qty}**`).join("\n");
}

async function deleteAllLines(commitmentRecordId) {
  const rows = await linesTable
    .select({
      filterByFormula: `{${F.LINE_COMMITMENT_RECORD_ID}} = '${escapeForFormula(commitmentRecordId)}'`,
      maxRecords: 200,
    })
    .firstPage();

  if (!rows.length) return;

  const ids = rows.map((r) => r.id);
  const chunkSize = 10;
  for (let i = 0; i < ids.length; i += chunkSize) {
    await linesTable.destroy(ids.slice(i, i + chunkSize));
  }
}

function buildOrFormula(fieldName, values) {
  const parts = values.map((v) => `{${fieldName}} = '${escapeForFormula(v)}'`);
  if (!parts.length) return "FALSE()";
  if (parts.length === 1) return parts[0];
  return `OR(${parts.join(",")})`;
}

function normalizeDiscountPctToDecimal(raw) {
  // supports values like 2 (meaning 2%) or 0.02
  const n = Number(raw);
  if (!Number.isFinite(n)) return 0;
  if (n > 1) return n / 100;
  if (n < 0) return 0;
  return n;
}

async function findRuleSetForOpportunity(oppRecordId) {
  // We load rule sets and match by linked record id inclusion.
  const sets = await tierRuleSetsTable.select({ maxRecords: 200 }).all();
  for (const s of sets) {
    const oppLinks = s.fields?.[F.TRS_OPPORTUNITIES];
    if (Array.isArray(oppLinks) && oppLinks.includes(oppRecordId)) return s;
  }
  return null;
}

async function fetchTierRulesByIds(ruleIds) {
  const ids = (ruleIds || []).filter(Boolean);
  if (!ids.length) return [];

  // Fetch in parallel; keep it robust
  const records = await Promise.all(ids.map((id) => tierRulesTable.find(id).catch(() => null)));

  return records
    .filter(Boolean)
    .map((r) => ({
      id: r.id,
      minPairs: Number(r.fields?.[F.TR_MIN_PAIRS] || 0),
      discount: normalizeDiscountPctToDecimal(r.fields?.[F.TR_DISCOUNT_PCT] ?? 0),
    }))
    .filter((t) => Number.isFinite(t.minPairs) && t.minPairs >= 0)
    .sort((a, b) => a.minPairs - b.minPairs);
}

async function fetchTiersForOpportunity(oppRecordId) {
  const ruleSet = await findRuleSetForOpportunity(oppRecordId);
  if (!ruleSet) return [];
  const ruleIds = ruleSet.fields?.[F.TRS_TIER_RULES];
  return await fetchTierRulesByIds(ruleIds);
}

async function recalcOpportunityPricing(oppRecordId, totalPairs) {
  // Updates Opportunity fields based on the tier rules of the rule-set linked to this opportunity.
  // Safe by design.
  try {
    const opp = await oppsTable.find(oppRecordId);
    const oppFields = opp.fields || {};

    const startPriceRaw = oppFields[F.OPP_START_SELL_PRICE];
    const startPrice = Number(asText(startPriceRaw));

    const tiers = await fetchTiersForOpportunity(oppRecordId);
    if (!tiers.length) {
      // No tier config found -> clear next tier hints
      await oppsTable.update(oppRecordId, {
        [F.OPP_NEXT_MIN_PAIRS]: null,
        [F.OPP_NEXT_DISCOUNT]: null,
      });
      return;
    }

    // Current tier = highest tier with minPairs <= totalPairs
    let current = tiers[0];
    for (const t of tiers) {
      if (totalPairs >= t.minPairs) current = t;
      else break;
    }

    const next = tiers.find((t) => t.minPairs > totalPairs) || null;

    let currentSellPrice = null;
    if (Number.isFinite(startPrice)) {
      currentSellPrice = startPrice * (1 - (current.discount || 0));
    }

    await oppsTable.update(oppRecordId, {
      [F.OPP_CURRENT_DISCOUNT]: current.discount || 0,
      [F.OPP_CURRENT_SELL_PRICE]: currentSellPrice ?? null,
      [F.OPP_NEXT_MIN_PAIRS]: next ? next.minPairs : null,
      [F.OPP_NEXT_DISCOUNT]: next ? next.discount : null,
    });
  } catch (err) {
    console.warn("⚠️ recalcOpportunityPricing skipped/error:", err?.message || err);
  }
}

async function recalcOpportunityTotals(oppRecordId) {
  // 1) Find counted commitments for this opportunity
  const statusOr = buildOrFormula(F.COM_STATUS, Array.from(COUNTED_STATUSES));
  const commitments = await commitmentsTable
    .select({
      filterByFormula: `AND({${F.COM_OPP_RECORD_ID}} = '${escapeForFormula(oppRecordId)}', ${statusOr})`,
      maxRecords: 1000,
    })
    .all();

  const commitmentIds = commitments.map((r) => r.id);
  if (!commitmentIds.length) {
    await oppsTable.update(oppRecordId, { [F.OPP_CURRENT_TOTAL_PAIRS]: 0 });
    await recalcOpportunityPricing(oppRecordId, 0);
    return 0;
  }

  // 2) Sum quantities for all lines belonging to those commitments (chunked)
  const chunkSize = 25;
  let total = 0;

  for (let i = 0; i < commitmentIds.length; i += chunkSize) {
    const chunk = commitmentIds.slice(i, i + chunkSize);
    const orChunk = buildOrFormula(F.LINE_COMMITMENT_RECORD_ID, chunk);

    const lines = await linesTable
      .select({
        filterByFormula: `${orChunk}`,
        maxRecords: 1000,
      })
      .all();

    for (const line of lines) {
      const q = Number(line.fields[F.LINE_QTY] || 0);
      if (Number.isFinite(q) && q > 0) total += q;
    }
  }

  // 3) Write total back
  await oppsTable.update(oppRecordId, { [F.OPP_CURRENT_TOTAL_PAIRS]: total });

  // 4) Update pricing tiers
  await recalcOpportunityPricing(oppRecordId, total);

  return total;
}

function buildSizeButtons(opportunityRecordId, sizes, opts = {}) {
  const { status = "Draft" } = opts;

  const isEditable = EDITABLE_STATUSES.has(status);
  const isSubmitted = status === "Submitted";
  const isHardLocked = HARD_LOCKED_STATUSES.has(status);

  const rows = [];
  let row = new ActionRowBuilder();
  let inRow = 0;

  for (const s of sizes) {
    if (rows.length === 4) break;
    if (inRow === 5) {
      rows.push(row);
      row = new ActionRowBuilder();
      inRow = 0;
    }

    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`size_pick:${opportunityRecordId}:${sizeKeyEncode(s)}`)
        .setLabel(s)
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(!isEditable)
    );
    inRow++;
  }
  if (inRow > 0 && rows.length < 4) rows.push(row);

  const controls = new ActionRowBuilder();

  controls.addComponents(
    new ButtonBuilder()
      .setCustomId(`cart_review:${opportunityRecordId}`)
      .setLabel("Review")
      .setStyle(ButtonStyle.Primary)
  );

  if (isEditable) {
    controls.addComponents(
      new ButtonBuilder()
        .setCustomId(`cart_submit:${opportunityRecordId}`)
        .setLabel("Submit")
        .setStyle(ButtonStyle.Success),
      new ButtonBuilder()
        .setCustomId(`cart_clear:${opportunityRecordId}`)
        .setLabel("Clear")
        .setStyle(ButtonStyle.Danger)
    );
  } else if (isSubmitted) {
    controls.addComponents(
      new ButtonBuilder()
        .setCustomId(`cart_addmore:${opportunityRecordId}`)
        .setLabel("Add More")
        .setStyle(ButtonStyle.Success)
    );
  } else if (isHardLocked) {
    controls.addComponents(
      new ButtonBuilder()
        .setCustomId(`cart_locked:${opportunityRecordId}`)
        .setLabel("Locked")
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(true)
    );
  }

  rows.push(controls);
  return rows;
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

// Single source of truth for DM panel updates
async function refreshDmPanel(oppRecordId, commitmentRecordId) {
  const opp = await oppsTable.find(oppRecordId);
  const oppFields = opp.fields || {};

  const oppEmbed = buildOpportunityEmbed(oppFields);

  const freshCommitment = await commitmentsTable.find(commitmentRecordId);
  const dmChannelId = asText(freshCommitment.fields[F.COM_DM_CHANNEL_ID]);
  const dmMessageId = asText(freshCommitment.fields[F.COM_DM_MESSAGE_ID]);
  const status = asText(freshCommitment.fields[F.COM_STATUS]) || "Draft";

  const cartEmbed = new EmbedBuilder()
    .setTitle("🧾 Bulk Cart")
    .setDescription((await getCartLinesText(commitmentRecordId)) + `

**Status:** **${status}**`)
    .setColor(0xffd300);

  const sizes = await resolveAllowedSizesAndMaybeWriteback(oppRecordId, oppFields);
  const components = sizes.length ? buildSizeButtons(oppRecordId, sizes, { status }) : [];

  if (dmChannelId && dmMessageId) {
    const ch = await client.channels.fetch(dmChannelId);
    const msg = await ch.messages.fetch(dmMessageId);
    await msg.edit({
      embeds: [oppEmbed, cartEmbed],
      components: components.length ? components : [],
    });
  }
}

function deferEphemeralIfGuild(inGuild) {
  return inGuild ? { flags: MessageFlags.Ephemeral } : {};
}

client.on(Events.InteractionCreate, async (interaction) => {
  const inGuild = !!interaction.guildId;

  /* ---------- Join Bulk (from public channel) ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("opp_join:")) {
    const opportunityRecordId = interaction.customId.split("opp_join:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const opp = await oppsTable.find(opportunityRecordId);
      const oppFields = opp.fields || {};

      const buyer = await upsertBuyer(interaction.user);

      // reuse commitment if exists, else create
      let commitment = await findLatestCommitment(interaction.user.id, opportunityRecordId);
      if (!commitment) {
        commitment = await createCommitment({
          oppRecordId: opportunityRecordId,
          buyerRecordId: buyer.id,
          discordId: interaction.user.id,
          discordTag: interaction.user.tag,
        });
      }

      const freshCommitment = await commitmentsTable.find(commitment.id);
      const status = asText(freshCommitment.fields[F.COM_STATUS]) || "Draft";
      const storedDmChannelId = asText(freshCommitment.fields[F.COM_DM_CHANNEL_ID]);
      const storedDmMessageId = asText(freshCommitment.fields[F.COM_DM_MESSAGE_ID]);

      const dm = await interaction.user.createDM();

      const oppEmbed = buildOpportunityEmbed(oppFields);
      const cartEmbed = new EmbedBuilder()
        .setTitle("🧾 Bulk Cart")
        .setDescription((await getCartLinesText(commitment.id)) + `

**Status:** **${status}**`)
        .setColor(0xffd300);

      const sizes = await resolveAllowedSizesAndMaybeWriteback(opportunityRecordId, oppFields);
      const components = sizes.length ? buildSizeButtons(opportunityRecordId, sizes, { status }) : [];

      // Update existing panel if we have ids, otherwise send new
      let msg;
      if (storedDmChannelId && storedDmMessageId) {
        try {
          const ch = await client.channels.fetch(storedDmChannelId);
          msg = await ch.messages.fetch(storedDmMessageId);
          await msg.edit({
            embeds: [oppEmbed, cartEmbed],
            components: components.length ? components : [],
          });
        } catch (_) {
          msg = await dm.send({
            embeds: [oppEmbed, cartEmbed],
            components: components.length ? components : undefined,
          });
        }
      } else {
        msg = await dm.send({
          embeds: [oppEmbed, cartEmbed],
          components: components.length ? components : undefined,
        });
      }

      await updateCommitmentDM(commitment.id, dm.id, msg.id);

      await interaction.editReply("✅ I’ve sent you a DM to build your cart.");
      return;
    } catch (err) {
      console.error("opp_join handler error:", err);
      await interaction.editReply(
        "⚠️ I couldn’t DM you. Please enable DMs for this server (Privacy Settings) and try again."
      );
      return;
    }
  }

  /* ---------- Add More (Submitted -> Editing) ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("cart_addmore:")) {
    const oppRecordId = interaction.customId.split("cart_addmore:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
      if (!commitment) {
        await interaction.editReply("🧾 No cart found yet.");
        return;
      }

      const status = await getCommitmentStatus(commitment.id);
      if (status !== "Submitted") {
        await interaction.editReply("⚠️ Add More is only available after you submit.");
        return;
      }

      await touchCommitment(commitment.id, { [F.COM_STATUS]: "Editing" });
      // totals don’t change here, but we refresh panel
      await refreshDmPanel(oppRecordId, commitment.id);

      await interaction.editReply("✅ Editing enabled. Add more sizes and press Submit again to confirm.");
      return;
    } catch (err) {
      console.error("cart_addmore error:", err);
      await interaction.editReply("⚠️ Could not enable editing. Try again.");
      return;
    }
  }

  /* ---------- Locked indicator (no-op) ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("cart_locked:")) {
    await interaction.reply({
      content: "🔒 This commitment is locked. Contact staff if you need changes.",
      ...deferEphemeralIfGuild(inGuild),
    });
    return;
  }

  /* ---------- Size button -> modal ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("size_pick:")) {
    const [, oppRecordId, encodedSize] = interaction.customId.split(":");
    const size = sizeKeyDecode(encodedSize);

    const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
    if (commitment) {
      const status = await getCommitmentStatus(commitment.id);
      if (!EDITABLE_STATUSES.has(status)) {
        await interaction.reply({ content: "⚠️ This commitment is not editable right now." });
        return;
      }
    }

    const modal = new ModalBuilder()
      .setCustomId(`qty_modal:${oppRecordId}:${encodedSize}`)
      .setTitle(`Quantity for ${size}`);

    const qtyInput = new TextInputBuilder()
      .setCustomId("qty")
      .setLabel("Quantity (0 to remove)")
      .setStyle(TextInputStyle.Short)
      .setRequired(true);

    modal.addComponents(new ActionRowBuilder().addComponents(qtyInput));
    await interaction.showModal(modal);
    return;
  }

  /* ---------- Modal submit -> upsert line (increase-only in Editing) + refresh DM panel ---------- */
  if (interaction.isModalSubmit() && interaction.customId.startsWith("qty_modal:")) {
    // ACK immediately to avoid Discord 10062 (Unknown interaction) if Airtable/DM updates take >3s
    await interaction.deferReply();
    const [, oppRecordId, encodedSize] = interaction.customId.split(":");
    const size = sizeKeyDecode(encodedSize);

    const qtyRaw = interaction.fields.getTextInputValue("qty");
    const qty = Number.parseInt(qtyRaw, 10);

    if (!Number.isFinite(qty) || qty < 0 || qty > 999) {
      await interaction.editReply({ content: "⚠️ Please enter a valid quantity (0–999)." });
      return;
    }

    // Find / create commitment
    let commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
    if (!commitment) {
      const buyer = await upsertBuyer(interaction.user);
      commitment = await createCommitment({
        oppRecordId,
        buyerRecordId: buyer.id,
        discordId: interaction.user.id,
        discordTag: interaction.user.tag,
      });
    }

    const statusNow = await getCommitmentStatus(commitment.id);

    // Only Draft/Editing can modify quantities
    if (!EDITABLE_STATUSES.has(statusNow)) {
      await interaction.editReply({ content: "⚠️ This commitment is not editable right now." });
      return;
    }

    // Increase-only policy in Editing:
    if (statusNow === "Editing") {
      const existingQty = await getLineQty(commitment.id, size);
      if (qty < existingQty) {
        await interaction.editReply({
          content:
            "⚠️ While editing after submission, you can only **increase** quantities. Contact staff to reduce/remove.",
        });
        return;
      }
    }

    await upsertLine(commitment.id, size, qty);
    await touchCommitment(commitment.id);
    await recalcOpportunityTotals(oppRecordId);
    await refreshDmPanel(oppRecordId, commitment.id);

    await interaction.editReply({ content: `✅ Saved: **${size} × ${qty}**` });
    return;
  }

  /* ---------- Review cart ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("cart_review:")) {
    const oppRecordId = interaction.customId.split("cart_review:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
      if (!commitment) {
        await interaction.editReply("🧾 No cart found yet. Pick a size first.");
        return;
      }

      const cartText = await getCartLinesText(commitment.id);
      await touchCommitment(commitment.id);

      await interaction.editReply(`Here’s your cart:

${cartText}`);
      return;
    } catch (err) {
      console.error("cart_review error:", err);
      await interaction.editReply("⚠️ Could not load your cart.");
      return;
    }
  }

  /* ---------- Clear cart (Draft only) ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("cart_clear:")) {
    const oppRecordId = interaction.customId.split("cart_clear:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
      if (!commitment) {
        await interaction.editReply("🧾 No cart found yet.");
        return;
      }

      const status = await getCommitmentStatus(commitment.id);
      if (status !== "Draft") {
        await interaction.editReply("⚠️ You can only clear while in Draft.");
        return;
      }

      await deleteAllLines(commitment.id);
      await touchCommitment(commitment.id, { [F.COM_STATUS]: "Draft" });
      await recalcOpportunityTotals(oppRecordId);
      await refreshDmPanel(oppRecordId, commitment.id);

      await interaction.editReply("🧹 Cleared your cart.");
      return;
    } catch (err) {
      console.error("cart_clear error:", err);
      await interaction.editReply("⚠️ Could not clear your cart.");
      return;
    }
  }

  /* ---------- Submit cart (Draft/Editing -> Submitted) ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("cart_submit:")) {
    const oppRecordId = interaction.customId.split("cart_submit:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
      if (!commitment) {
        await interaction.editReply("🧾 No cart found yet. Add sizes first.");
        return;
      }

      const status = await getCommitmentStatus(commitment.id);
      if (status !== "Draft" && status !== "Editing") {
        await interaction.editReply("⚠️ This commitment can’t be submitted right now.");
        return;
      }

      const cartText = await getCartLinesText(commitment.id);
      if (cartText.includes("No sizes selected")) {
        await interaction.editReply("⚠️ Your cart is empty. Add at least one size before submitting.");
        return;
      }

      await touchCommitment(commitment.id, {
        [F.COM_STATUS]: "Submitted",
        [F.COM_COMMITTED_AT]: new Date().toISOString(),
      });

      await recalcOpportunityTotals(oppRecordId);
      await refreshDmPanel(oppRecordId, commitment.id);

      await interaction.editReply(
        "✅ Submitted! Your commitment is now locked. Use **Add More** if you want to increase."
      );
      return;
    } catch (err) {
      console.error("cart_submit error:", err);
      await interaction.editReply("⚠️ Could not submit your cart.");
      return;
    }
  }
});

/* =========================
   Express (post + sync embeds)
========================= */

const app = express();
app.use(morgan("tiny"));
app.use(express.json());

app.get("/", async (_req, res) => {
  res.send("Bulk bot is live ✅");
});

app.get("/airtable-test", async (_req, res) => {
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
      new ButtonBuilder().setCustomId(`opp_join:${opportunityRecordId}`).setLabel("Join Bulk").setStyle(ButtonStyle.Success)
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
    if (fields["Posted At"] !== undefined) updatePayload["Posted At"] = new Date().toISOString();

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
      return res
        .status(400)
        .json({ ok: false, error: "Missing Discord Public Channel ID or Discord Public Message ID" });
    }

    const channel = await client.channels.fetch(String(channelId));
    if (!channel || !channel.isTextBased()) {
      return res.status(500).json({ ok: false, error: "Channel not found or not text-based" });
    }

    const message = await channel.messages.fetch(String(messageId));
    const embed = buildOpportunityEmbed(fields);

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`opp_join:${opportunityRecordId}`).setLabel("Join Bulk").setStyle(ButtonStyle.Success)
    );

    await message.edit({ embeds: [embed], components: [row] });
    return res.json({ ok: true, synced: true });
  } catch (err) {
    console.error("sync-opportunity error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post("/sync-opportunity-dms", async (req, res) => {
  try {
    const incomingSecret = req.header("x-post-secret") || "";
    if (!POST_OPP_SECRET || incomingSecret !== POST_OPP_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) {
      return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });
    }

    const rows = await commitmentsTable
      .select({
        maxRecords: 1000,
        filterByFormula: `AND(
          {${F.COM_OPP_RECORD_ID}} = '${escapeForFormula(opportunityRecordId)}',
          {${F.COM_DM_CHANNEL_ID}} != '',
          {${F.COM_DM_MESSAGE_ID}} != ''
        )`,
      })
      .all();

    let updated = 0;
    for (const c of rows) {
      try {
        await refreshDmPanel(opportunityRecordId, c.id);
        updated++;
      } catch (e) {
        console.warn("DM sync failed for commitment", c.id, e?.message || e);
      }
    }

    return res.json({ ok: true, synced: true, commitments_found: rows.length, dms_updated: updated });
  } catch (err) {
    console.error("sync-opportunity-dms error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.listen(LISTEN_PORT, () => console.log(`🌐 Listening on ${LISTEN_PORT}`));

client.login(DISCORD_TOKEN);
