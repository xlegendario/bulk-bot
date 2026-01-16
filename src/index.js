// FULL PRODUCTION SCRIPT (stable)
// Includes: public post+sync, DM cart builder, counted quantity snapshot, tier engine (Rule Sets+Rules),
// close/finalize with per-buyer deal channels, staff deposit confirm button, supplier quote + confirmed bulks summary.

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
  ChannelType,
  PermissionsBitField,
} from "discord.js";

/* =========================
   SAFETY
========================= */
process.on("unhandledRejection", (err) => {
  console.error("Unhandled promise rejection:", err);
});

const NL = "\n";

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
  AIRTABLE_CONFIRMED_BULKS_TABLE = "Confirmed Bulks",

  // Tier engine tables
  AIRTABLE_TIER_RULES_TABLE = "Tier Rules",
  AIRTABLE_TIER_RULE_SETS_TABLE = "Tier Rule Sets",

  BULK_PUBLIC_CHANNEL_ID,
  POST_OPP_SECRET,

  // Discord ops
  BULK_GUILD_ID, // optional; if omitted we infer from BULK_PUBLIC_CHANNEL_ID
  SUPPLIER_QUOTES_CHANNEL_ID, // staff-only channel for supplier quote
  CONFIRMED_BULKS_CHANNEL_ID, // staff-only channel for confirmed bulks summary
  STAFF_ROLE_IDS, // optional comma-separated role IDs
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
if (!BULK_PUBLIC_CHANNEL_ID) {
  console.error("❌ BULK_PUBLIC_CHANNEL_ID missing");
  process.exit(1);
}

/* =========================
   AIRTABLE
========================= */

const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);
const buyersTable = base(AIRTABLE_BUYERS_TABLE);
const oppsTable = base(AIRTABLE_OPPS_TABLE);
const commitmentsTable = base(AIRTABLE_COMMITMENTS_TABLE);
const linesTable = base(AIRTABLE_LINES_TABLE);
const sizePresetsTable = base(AIRTABLE_SIZE_PRESETS_TABLE);
const tierRulesTable = base(AIRTABLE_TIER_RULES_TABLE);
const tierRuleSetsTable = base(AIRTABLE_TIER_RULE_SETS_TABLE);
const confirmedBulksTable = base(AIRTABLE_CONFIRMED_BULKS_TABLE);

console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);

/* =========================
   FIELD CONSTANTS
========================= */

const F = {
  // Buyers
  BUYER_DISCORD_ID: "Discord User ID",
  BUYER_DISCORD_USERNAME: "Discord Username",
  BUYER_DEFAULT_DEPOSIT_PCT: "Default Deposit %",

  // Opportunities
  OPP_PRODUCT_NAME: "Product Name",
  OPP_SKU_SOFT: "SKU (Soft)",
  OPP_SKU: "SKU",
  OPP_MIN_SIZE: "Min Size",
  OPP_MAX_SIZE: "Max Size",
  OPP_CURRENCY: "Currency",
  OPP_START_SELL_PRICE: "Start Sell Price",
  OPP_CURRENT_SELL_PRICE: "Current Sell Price",
  OPP_CURRENT_DISCOUNT: "Current Discount %", // decimal 0.02 for 2%
  OPP_CURRENT_TOTAL_PAIRS: "Current Total Pairs",
  OPP_NEXT_MIN_PAIRS: "Next Tier Min Pairs",
  OPP_NEXT_DISCOUNT: "Next Tier Discount %", // decimal
  OPP_PICTURE: "Picture",
  OPP_ALLOWED_SIZES: "Allowed Sizes (Generated)",
  OPP_STATUS: "Status", // Draft/Open/Closed/Confirmed/Cancelled
  OPP_DISCORD_CATEGORY_ID: "Discord Category ID",
  OPP_QUOTES_MESSAGE_ID: "Discord Quotes Message ID",
  OPP_DEPOSIT_DUE_AT: "Deposit Due At",
  OPP_SUPPLIER_LINK: "Supplier", // linked field on Opportunities

  // New: supplier + final snapshot fields
  OPP_SUPPLIER_UNIT_PRICE: "Supplier Price",
  OPP_FINAL_TOTAL_PAIRS: "Final Total Pairs",
  OPP_FINAL_SELL_PRICE: "Final Sell Price",
  OPP_FINAL_DISCOUNT_PCT: "Final Discount %",

  // Commitments
  COM_OPPORTUNITY: "Opportunity",
  COM_BUYER: "Buyer",
  COM_STATUS: "Status",
  COM_DISCORD_USER_ID: "Discord User ID",
  COM_DISCORD_USER_TAG: "Discord User Tag",
  COM_DM_CHANNEL_ID: "Discord Private Channel ID",
  COM_DM_MESSAGE_ID: "Discord Summary Message ID",
  COM_LAST_ACTIVITY: "Last Activity At",
  COM_OPP_RECORD_ID: "Opportunity Record ID",
  COM_COMMITTED_AT: "Committed At",
  COM_LAST_ACTION: "Last Action",
  COM_DEAL_CHANNEL_ID: "Discord Deal Channel ID",
  COM_DEAL_MESSAGE_ID: "Discord Deal Message ID",
  COM_FINAL_UNIT_PRICE: "Final Unit Price", // currency field on Commitments
  COM_DEPOSIT_PCT: "Deposit %",

  // Lines
  LINE_COMMITMENT: "Commitment",
  LINE_COMMITMENT_RECORD_ID: "Commitment Record ID",
  LINE_SIZE: "Size",
  LINE_QTY: "Quantity",
  LINE_COUNTED_QTY: "Counted Quantity",

  // Size presets
  PRESET_SIZE_LADDER: "Size Ladder",
  PRESET_LINKED_SKUS: "Linked SKU's",

  // Tier rules
  TR_MIN_PAIRS: "Min Pairs",
  TR_DISCOUNT_PCT: "Discount %",

  // Tier rule sets
  TRS_OPPORTUNITIES: "Opportunities",
  TRS_TIER_RULES: "Tier Rules",

  // Confirmed Bulks
  CB_LINKED_OPPORTUNITY: "Linked Opportunity",
  CB_LINKED_COMMITMENTS: "Linked Commitments",
  CB_LINKED_BUYERS: "Linked Buyers",
  CB_LINKED_SUPPLIER: "Linked Supplier", // linked field on Confirmed Bulks
  
};

/* =========================
   STATUS RULES
========================= */

const EDITABLE_STATUSES = new Set(["Draft", "Editing"]);
const HARD_LOCKED_STATUSES = new Set(["Locked", "Deposit Paid", "Paid", "Cancelled"]);

// We do NOT count Editing anymore. We count only the last submitted snapshot via Counted Quantity.
const COUNTED_STATUSES = new Set(["Submitted", "Locked", "Deposit Paid", "Paid"]);

/* =========================
   HELPERS
========================= */

const escapeForFormula = (s) => String(s ?? "").replace(/'/g, "\\'");

function parseCsvIds(v) {
  return String(v || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

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

function getCommitmentIdSuffix(commitmentFields) {
  // Adjust this field name if your Airtable column is named differently
  const commitmentId = asText(commitmentFields?.["Commitment ID"]).trim();
  if (!commitmentId) return null;

  // Take whatever is after the last "-"
  const parts = commitmentId.split("-").map((p) => p.trim()).filter(Boolean);
  return parts.length ? parts[parts.length - 1] : null;
}

function toBulkId(opportunityIdOrFallback) {
  const raw = asText(opportunityIdOrFallback).trim();
  if (!raw) return "";
  // OPP-2026-0006 -> BULK-2026-0006
  return raw.startsWith("OPP-") ? `BULK-${raw.slice(4)}` : raw;
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

function parseMoneyNumber(v) {
  // Handles Airtable currency fields that may come through as "€173,25" or "173.25" or "173,25".
  const raw = asText(v).trim();
  if (!raw) return null;
  // Keep digits, comma, dot, minus
  const cleaned = raw.replace(/[^0-9,.-]/g, "");
  if (!cleaned) return null;
  // If both comma and dot exist, assume dot is thousands separator and comma is decimal.
  let normalized = cleaned;
  if (cleaned.includes(",") && cleaned.includes(".")) {
    normalized = cleaned.replace(/\./g, "").replace(/,/g, ".");
  } else if (cleaned.includes(",") && !cleaned.includes(".")) {
    normalized = cleaned.replace(/,/g, ".");
  }
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function formatPercent(v) {
  const raw = asText(v);
  if (raw === "") return "—";
  const num = Number(raw);
  if (Number.isNaN(num)) return "—";
  const pct = (num * 100).toFixed(2).replace(/\.00$/, "");
  return `${pct}%`;
}

function normalizeDiscountPctToDecimal(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return 0;
  if (n > 1) return n / 100;
  if (n < 0) return 0;
  return n;
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
  return asText(v).split(",").map((s) => s.trim()).filter(Boolean);
}

function sliceLadderByMinMax(ladder, minRaw, maxRaw) {
  const min = String(minRaw || "").trim();
  const max = String(maxRaw || "").trim();
  if (!min && !max) return ladder;

  const i1 = min ? ladder.indexOf(min) : -1;
  const i2 = max ? ladder.indexOf(max) : -1;

  if ((min && i1 === -1) || (max && i2 === -1)) return ladder;

  if (min && !max) return ladder.slice(i1);
  if (!min && max) return ladder.slice(0, i2 + 1);

  const start = Math.min(i1, i2);
  const end = Math.max(i1, i2);
  return ladder.slice(start, end + 1);
}

async function setFinalUnitPriceForOpportunityCommitments(opportunityRecordId, unitPriceNumber) {
  if (!Number.isFinite(unitPriceNumber)) return;

  const rows = await commitmentsTable
    .select({
      maxRecords: 1000,
      filterByFormula: `{${F.COM_OPP_RECORD_ID}}='${escapeForFormula(opportunityRecordId)}'`,
    })
    .all();

  const updates = rows
    .map((r) => ({ id: r.id, fields: { [F.COM_FINAL_UNIT_PRICE]: unitPriceNumber } }))
    .filter((u) => u.id);

  for (let i = 0; i < updates.length; i += 10) {
    await commitmentsTable.update(updates.slice(i, i + 10));
  }
}


function buildOrFormula(fieldName, values) {
  const parts = values.map((v) => `{${fieldName}} = '${escapeForFormula(v)}'`);
  if (!parts.length) return "FALSE()";
  if (parts.length === 1) return parts[0];
  return `OR(${parts.join(",")})`;
}

function safeChannelName(usernameOrTag) {
  const raw = String(usernameOrTag || "buyer").toLowerCase();
  const base = raw
    .replace(/#[0-9]{4}$/g, "")
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 60);
  return base || "buyer";
}

function deferEphemeralIfGuild(inGuild) {
  return inGuild ? { flags: MessageFlags.Ephemeral } : {};
}

function scheduleDeleteInteractionReply(interaction, ms = 2000) {
  setTimeout(() => interaction.deleteReply().catch(() => {}), ms);
}

/* =========================
   DISCORD CLIENT
========================= */

const client = new Client({
  intents: [GatewayIntentBits.Guilds],
  partials: [Partials.Channel],
});

client.once(Events.ClientReady, async (c) => {
  console.log(`🤖 Logged in as ${c.user.tag}`);
});

async function inferGuildId() {
  if (BULK_GUILD_ID) return String(BULK_GUILD_ID);
  const ch = await client.channels.fetch(String(BULK_PUBLIC_CHANNEL_ID));
  return ch?.guildId ? String(ch.guildId) : null;
}

async function getGuildMe(guild) {
  try {
    return await guild.members.fetchMe();
  } catch {
    return guild.members.me;
  }
}

function isStaffMember(member) {
  if (!member) return false;
  if (member.permissions?.has?.(PermissionsBitField.Flags.Administrator)) return true;
  const staffRoleIds = new Set(parseCsvIds(STAFF_ROLE_IDS));
  return member.roles?.cache?.some((r) => staffRoleIds.has(r.id)) || false;
}

/* =========================
   AIRTABLE: BUYERS / COMMITMENTS / LINES
========================= */

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
    } catch {}
    return existing[0];
  }

  return await buyersTable.create({
    [F.BUYER_DISCORD_ID]: discordId,
    [F.BUYER_DISCORD_USERNAME]: username,
  });
}

async function createCommitment({ oppRecordId, buyerRecordId, discordId, discordTag }) {
  const nowIso = new Date().toISOString();
  return await commitmentsTable.create({
    [F.COM_OPPORTUNITY]: [oppRecordId],
    [F.COM_BUYER]: [buyerRecordId],
    [F.COM_STATUS]: "Draft",
    [F.COM_DISCORD_USER_ID]: discordId,
    [F.COM_DISCORD_USER_TAG]: discordTag,
    [F.COM_LAST_ACTIVITY]: nowIso,
    [F.COM_OPP_RECORD_ID]: oppRecordId,
  });
}

async function touchCommitment(commitmentRecordId, patch = {}) {
  const payload = { [F.COM_LAST_ACTIVITY]: new Date().toISOString(), ...patch };
  try {
    await commitmentsTable.update(commitmentRecordId, payload);
  } catch (err) {
    if (String(err?.error) === "UNKNOWN_FIELD_NAME" && payload[F.COM_LAST_ACTION] !== undefined) {
      const { [F.COM_LAST_ACTION]: _ignored, ...rest } = payload;
      await commitmentsTable.update(commitmentRecordId, rest);
      return;
    }
    throw err;
  }
}

async function updateCommitmentDM(commitmentRecordId, dmChannelId, dmMessageId) {
  await commitmentsTable.update(commitmentRecordId, {
    [F.COM_DM_CHANNEL_ID]: String(dmChannelId),
    [F.COM_DM_MESSAGE_ID]: String(dmMessageId),
    [F.COM_LAST_ACTIVITY]: new Date().toISOString(),
  });
}

async function findLatestCommitment(discordUserId, oppRecordId) {
  const rows = await commitmentsTable
    .select({
      maxRecords: 1,
      sort: [{ field: "Created At", direction: "desc" }],
      filterByFormula: `AND({${F.COM_DISCORD_USER_ID}}='${escapeForFormula(discordUserId)}',{${F.COM_OPP_RECORD_ID}}='${escapeForFormula(oppRecordId)}')`,
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
      filterByFormula: `AND({${F.LINE_COMMITMENT_RECORD_ID}}='${escapeForFormula(commitmentRecordId)}',{${F.LINE_SIZE}}='${escapeForFormula(size)}')`,
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
      filterByFormula: `AND({${F.LINE_COMMITMENT_RECORD_ID}}='${escapeForFormula(commitmentRecordId)}',{${F.LINE_SIZE}}='${escapeForFormula(size)}')`,
    })
    .firstPage();
  if (!found.length) return 0;
  return Number(found[0].fields[F.LINE_QTY] || 0);
}

async function deleteAllLines(commitmentRecordId) {
  const rows = await linesTable
    .select({
      filterByFormula: `{${F.LINE_COMMITMENT_RECORD_ID}}='${escapeForFormula(commitmentRecordId)}'`,
      maxRecords: 200,
    })
    .firstPage();

  if (!rows.length) return;
  const ids = rows.map((r) => r.id);
  for (let i = 0; i < ids.length; i += 10) {
    await linesTable.destroy(ids.slice(i, i + 10));
  }
}

async function getCartLinesText(commitmentRecordId) {
  const rows = await linesTable
    .select({
      filterByFormula: `{${F.LINE_COMMITMENT_RECORD_ID}}='${escapeForFormula(commitmentRecordId)}'`,
      maxRecords: 200,
    })
    .firstPage();

  const items = rows
    .map((r) => ({ size: asText(r.fields[F.LINE_SIZE]), qty: Number(r.fields[F.LINE_QTY] || 0) }))
    .filter((x) => x.size && x.qty > 0)
    .sort((a, b) => a.size.localeCompare(b.size));

  if (!items.length) return "_No sizes selected yet._";
  return items.map((x) => `• **${x.size}** × **${x.qty}**`).join(NL);
}

async function snapshotCountedQuantities(commitmentRecordId) {
  const rows = await linesTable
    .select({
      filterByFormula: `{${F.LINE_COMMITMENT_RECORD_ID}}='${escapeForFormula(commitmentRecordId)}'`,
      maxRecords: 200,
    })
    .firstPage();

  if (!rows.length) return;

  const updates = rows.map((r) => ({
    id: r.id,
    fields: { [F.LINE_COUNTED_QTY]: Number(r.fields?.[F.LINE_QTY] || 0) },
  }));

  for (let i = 0; i < updates.length; i += 10) {
    await linesTable.update(updates.slice(i, i + 10));
  }
}

/* =========================
   SIZE PRESETS
========================= */

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

async function resolveAllowedSizesAndMaybeWriteback(oppRecordId, oppFields) {
  const existing = parseSizeList(oppFields[F.OPP_ALLOWED_SIZES]);
  if (existing.length) return existing;

  const sku = asText(oppFields[F.OPP_SKU_SOFT]) || asText(oppFields[F.OPP_SKU]);
  const ladder = await getPresetLadderBySku(sku);
  if (!ladder.length) return [];

  const sliced = sliceLadderByMinMax(ladder, oppFields[F.OPP_MIN_SIZE], oppFields[F.OPP_MAX_SIZE]);
  if (!sliced.length) return [];

  await oppsTable.update(oppRecordId, { [F.OPP_ALLOWED_SIZES]: sliced.join(", ") });
  return sliced;
}

function sizeKeyEncode(size) {
  return encodeURIComponent(size).replace(/%/g, "_");
}
function sizeKeyDecode(key) {
  return decodeURIComponent(key.replace(/_/g, "%"));
}

/* =========================
   TIER ENGINE (RULE SETS -> RULES)
========================= */

async function findRuleSetForOpportunity(oppRecordId) {
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
  try {
    const opp = await oppsTable.find(oppRecordId);
    const oppFields = opp.fields || {};
    const startPrice = Number(asText(oppFields[F.OPP_START_SELL_PRICE]));

    const tiers = await fetchTiersForOpportunity(oppRecordId);
    if (!tiers.length) {
      await oppsTable.update(oppRecordId, { [F.OPP_NEXT_MIN_PAIRS]: null, [F.OPP_NEXT_DISCOUNT]: null });
      return;
    }

    const effectivePairs = Math.max(Number(totalPairs || 0), 1);

    let current = tiers[0];
    for (const t of tiers) {
      if (effectivePairs >= t.minPairs) current = t;
      else break;
    }

    const next = tiers.find((t) => t.minPairs > effectivePairs) || null;

    const currentSellPrice = Number.isFinite(startPrice) ? startPrice * (1 - (current.discount || 0)) : null;

    await oppsTable.update(oppRecordId, {
      [F.OPP_CURRENT_DISCOUNT]: current.discount || 0,
      [F.OPP_CURRENT_SELL_PRICE]: currentSellPrice ?? null,
      [F.OPP_NEXT_MIN_PAIRS]: next ? next.minPairs : null,
      [F.OPP_NEXT_DISCOUNT]: next ? next.discount : null,
    });
  } catch (err) {
    console.warn("⚠️ recalcOpportunityPricing error:", err?.message || err);
  }
}

async function recalcOpportunityTotals(oppRecordId) {
  const statusOr = buildOrFormula(F.COM_STATUS, Array.from(COUNTED_STATUSES));

  const commitments = await commitmentsTable
    .select({
      filterByFormula: `AND({${F.COM_OPP_RECORD_ID}}='${escapeForFormula(oppRecordId)}', ${statusOr})`,
      maxRecords: 1000,
    })
    .all();

  const commitmentIds = commitments.map((r) => r.id);
  if (!commitmentIds.length) {
    await oppsTable.update(oppRecordId, { [F.OPP_CURRENT_TOTAL_PAIRS]: 0 });
    await recalcOpportunityPricing(oppRecordId, 0);
    return 0;
  }

  let total = 0;

  for (let i = 0; i < commitmentIds.length; i += 25) {
    const chunk = commitmentIds.slice(i, i + 25);
    const orChunk = buildOrFormula(F.LINE_COMMITMENT_RECORD_ID, chunk);

    const lines = await linesTable
      .select({
        filterByFormula: `${orChunk}`,
        maxRecords: 1000,
      })
      .all();

    for (const line of lines) {
      const q = Number(line.fields?.[F.LINE_COUNTED_QTY] ?? 0);
      if (Number.isFinite(q) && q > 0) total += q;
    }
  }

  await oppsTable.update(oppRecordId, { [F.OPP_CURRENT_TOTAL_PAIRS]: total });
  await recalcOpportunityPricing(oppRecordId, total);
  return total;
}

/* =========================
   EMBEDS / UI
========================= */

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
    "",
    `**Current Price:** **${currentPrice}**`,
    `**Current Discount:** **${currentDiscount}**`,
    `**Current Total Pairs:** **${currentTotalPairs}**`,
    "",
    `**MOQ for Next Tier:** **${nextMinPairs}**`,
    `**Next Tier Discount:** **${nextDiscount}**`,
  ].join(NL);

  const title = productName.length > 256 ? productName.slice(0, 253) + "..." : productName;

  const embed = new EmbedBuilder()
    .setTitle(title)
    .setDescription(desc)
    .setFooter({ text: "Join with any quantity • Price locks when bulk closes" })
    .setColor(0xffd300);

  if (picUrl) embed.setThumbnail(picUrl);
  return embed;
}

function buildCartEmbed(linesText, status, lastAction) {
  const statusLine = `${NL}${NL}**Status:** **${status}**`;
  const lastLine = lastAction ? `${NL}${NL}**Last update:** ${lastAction}` : "";
  return new EmbedBuilder().setTitle("🧾 Bulk Cart").setDescription(linesText + statusLine + lastLine).setColor(0xffd300);
}

function buildJoinRow(opportunityRecordId, disabled, label) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`opp_join:${opportunityRecordId}`).setLabel(label).setStyle(ButtonStyle.Success).setDisabled(!!disabled)
  );
}

function buildSizeButtons(opportunityRecordId, sizes, status) {
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
      new ButtonBuilder().setCustomId(`size_pick:${opportunityRecordId}:${sizeKeyEncode(s)}`).setLabel(s).setStyle(ButtonStyle.Secondary).setDisabled(!isEditable)
    );
    inRow++;
  }

  if (inRow > 0 && rows.length < 4) rows.push(row);

  const controls = new ActionRowBuilder();

  if (isEditable) {
    controls.addComponents(
      new ButtonBuilder().setCustomId(`cart_submit:${opportunityRecordId}`).setLabel("Submit").setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId(`cart_clear:${opportunityRecordId}`).setLabel("Clear").setStyle(ButtonStyle.Danger)
    );
  } else if (isSubmitted) {
    controls.addComponents(new ButtonBuilder().setCustomId(`cart_addmore:${opportunityRecordId}`).setLabel("Add More").setStyle(ButtonStyle.Success));
  } else if (isHardLocked) {
    controls.addComponents(
      new ButtonBuilder().setCustomId(`cart_locked:${opportunityRecordId}`).setLabel("Locked").setStyle(ButtonStyle.Secondary).setDisabled(true)
    );
  }

  rows.push(controls);
  return rows;
}

/* =========================
   DM PANEL UPDATE
========================= */

async function refreshDmPanel(oppRecordId, commitmentRecordId) {
  const opp = await oppsTable.find(oppRecordId);
  const oppFields = opp.fields || {};

  const freshCommitment = await commitmentsTable.find(commitmentRecordId);
  const dmChannelId = asText(freshCommitment.fields[F.COM_DM_CHANNEL_ID]);
  const dmMessageId = asText(freshCommitment.fields[F.COM_DM_MESSAGE_ID]);
  const status = asText(freshCommitment.fields[F.COM_STATUS]) || "Draft";
  const lastAction = asText(freshCommitment.fields[F.COM_LAST_ACTION]);

  if (!dmChannelId || !dmMessageId) return;

  const sizes = await resolveAllowedSizesAndMaybeWriteback(oppRecordId, oppFields);
  const components = sizes.length ? buildSizeButtons(oppRecordId, sizes, status) : [];

  const oppEmbed = buildOpportunityEmbed(oppFields);
  const linesText = await getCartLinesText(commitmentRecordId);
  const cartEmbed = buildCartEmbed(linesText, status, lastAction);

  const ch = await client.channels.fetch(dmChannelId);
  const msg = await ch.messages.fetch(dmMessageId);

  await msg.edit({ embeds: [oppEmbed, cartEmbed], components: components.length ? components : [] });
}

/* =========================
   DEAL CHANNELS + QUOTES
========================= */

async function ensureOppCategory(guild, oppRecordId, oppFields) {
  const existing = asText(oppFields[F.OPP_DISCORD_CATEGORY_ID]);
  if (existing) {
    try {
      const cat = await guild.channels.fetch(existing);
      if (cat) return cat;
    } catch {}
  }

  const name = (asText(oppFields["Opportunity ID"]) || oppRecordId).slice(0, 90);
  const cat = await guild.channels.create({ name, type: ChannelType.GuildCategory });

  await oppsTable.update(oppRecordId, { [F.OPP_DISCORD_CATEGORY_ID]: String(cat.id) });
  return cat;
}

async function computeCommitmentTotals(commitmentRecordId, oppFields) {
  const rows = await linesTable
    .select({
      filterByFormula: `{${F.LINE_COMMITMENT_RECORD_ID}}='${escapeForFormula(commitmentRecordId)}'`,
      maxRecords: 200,
    })
    .firstPage();

  const qtyTotal = rows.reduce((sum, r) => sum + Number(r.fields?.[F.LINE_QTY] || 0), 0);

  const currency = asText(oppFields[F.OPP_CURRENCY]) || "EUR";
  const unit = Number(asText(oppFields[F.OPP_CURRENT_SELL_PRICE] ?? oppFields[F.OPP_START_SELL_PRICE]));
  const unitPrice = Number.isFinite(unit) ? unit : 0;
  const totalAmount = qtyTotal * unitPrice;

  return { qtyTotal, currency, unitPrice, totalAmount };
}

function normalizePercentNumber(raw) {
  // Accepts: 50, "50", "50%", 0.5, "0.5", ["50%"]
  if (raw === undefined || raw === null) return null;

  const s = Array.isArray(raw) ? String(raw[0] ?? "").trim() : String(raw).trim();
  if (!s) return null;

  const cleaned = s.replace(/[^0-9.,-]/g, "");
  if (!cleaned) return null;

  const normalized = cleaned.includes(",") && !cleaned.includes(".")
    ? cleaned.replace(/,/g, ".")
    : cleaned;

  const n = Number(normalized);
  if (!Number.isFinite(n)) return null;

  // If stored as 0–1, convert to 0–100
  if (n > 0 && n <= 1) return n * 100;

  return n;
}

async function getBuyerDepositPct(commitmentFields) {
  // 1) Commitment override (Deposit % on the commitment record)
  const fromCommitment = normalizePercentNumber(commitmentFields?.[F.COM_DEPOSIT_PCT]);
  if (fromCommitment !== null) return fromCommitment;

  // 2) Buyer default
  const buyerLinks = commitmentFields?.[F.COM_BUYER];
  const buyerId = Array.isArray(buyerLinks) ? buyerLinks[0] : null;
  if (!buyerId) return 50;

  try {
    const b = await buyersTable.find(buyerId);
    const fromBuyer = normalizePercentNumber(b.fields?.[F.BUYER_DEFAULT_DEPOSIT_PCT]);
    return fromBuyer !== null ? fromBuyer : 50;
  } catch {
    return 50;
  }
}


async function ensureDealChannel({ guild, categoryId, buyerDiscordId, buyerTag, staffRoleIds, nameSuffix }) {
  const baseName = safeChannelName(buyerTag) + (nameSuffix ? `-${nameSuffix}` : "");
  const channelName = baseName.slice(0, 90);

  const me = await getGuildMe(guild);

  const overwrites = [
    { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
    {
      id: buyerDiscordId,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
      ],
    },
    {
      id: me.id,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
        PermissionsBitField.Flags.ManageMessages,
      ],
    },
  ];

  for (const rid of staffRoleIds) {
    overwrites.push({
      id: rid,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
        PermissionsBitField.Flags.ManageMessages,
      ],
    });
  }

  return await guild.channels.create({
    name: channelName,
    type: ChannelType.GuildText,
    parent: categoryId,
    permissionOverwrites: overwrites,
  });
}

function buildDepositEmbed({ oppFields, commitmentLinesText, currency, unitPrice, totalAmount, depositPct, depositDueAt }) {
  const product = asText(oppFields[F.OPP_PRODUCT_NAME]) || "Bulk";
  const sku = asText(oppFields[F.OPP_SKU_SOFT]) || asText(oppFields[F.OPP_SKU]) || "—";

  const sym = currencySymbol(currency);
  const unitStr = `${sym}${unitPrice % 1 === 0 ? unitPrice.toFixed(0) : unitPrice.toFixed(2)}`;
  const totalStr = `${sym}${totalAmount % 1 === 0 ? totalAmount.toFixed(0) : totalAmount.toFixed(2)}`;

  const depAmount = totalAmount * (depositPct / 100);
  const depStr = `${sym}${depAmount % 1 === 0 ? depAmount.toFixed(0) : depAmount.toFixed(2)}`;

  const desc =
    `**${product}**${NL}` +
    `**SKU:** \`${sku}\`${NL}${NL}` +
    `**Your commitment:**${NL}${commitmentLinesText}${NL}${NL}` +
    `**Unit price:** ${unitStr}${NL}` +
    `**Total:** ${totalStr}${NL}${NL}` +
    (depositPct <= 0
      ? `✅ **No deposit required for you.**${NL}`
      : `💳 **Deposit required (${depositPct}%):** **${depStr}**${NL}`) +
    (depositDueAt ? `${NL}⏰ **Deadline:** ${depositDueAt}${NL}` : "");

  return new EmbedBuilder().setTitle("📌 Bulk Payment / Confirmation").setDescription(desc).setColor(0xffd300);
}

function buildDepositButtonRow(commitmentId, enabled) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`deposit_confirm:${commitmentId}`).setLabel("Confirm Deposit Paid").setStyle(ButtonStyle.Success).setDisabled(!enabled)
  );
}

async function postSupplierQuote({ guild, oppRecordId, oppFields, sizeTotalsText, totalPairs, currency }) {
  if (!SUPPLIER_QUOTES_CHANNEL_ID) return null;

  const ch = await guild.channels.fetch(String(SUPPLIER_QUOTES_CHANNEL_ID)).catch(() => null);
  if (!ch || !ch.isTextBased()) return null;

  const oppId = asText(oppFields["Opportunity ID"]) || oppRecordId;
  const bulkId = toBulkId(oppId);

  const product = asText(oppFields[F.OPP_PRODUCT_NAME]) || "Bulk Opportunity";

  const supplierUnit = parseMoneyNumber(oppFields[F.OPP_SUPPLIER_UNIT_PRICE]);
  const supplierUnitStr =
    supplierUnit === null || Number.isNaN(supplierUnit) ? "—" : formatMoney(currency, supplierUnit);

  const supplierTotal =
    supplierUnit === null || Number.isNaN(supplierUnit) ? null : supplierUnit * totalPairs;
  const supplierTotalStr = supplierTotal === null ? "—" : formatMoney(currency, supplierTotal);

  const embed = new EmbedBuilder()
    .setTitle("📦 SUPPLIER QUOTE (BUY)")
    .setDescription(`**${bulkId}** — ${product}`)
    .setColor(0xffd300)
    .addFields(
      { name: "Confirmed Pairs", value: `**${totalPairs}**`, inline: true },
      { name: "Supplier Unit Price", value: supplierUnitStr, inline: true },
      { name: "Supplier Total", value: supplierTotalStr, inline: true },
      { name: "Size Breakdown", value: sizeTotalsText || "(none)", inline: false }
    );

  const existingMsgId = asText(oppFields[F.OPP_QUOTES_MESSAGE_ID]);
  if (existingMsgId) {
    try {
      const m = await ch.messages.fetch(existingMsgId);
      await m.edit({ content: "", embeds: [embed] });
      return m.id;
    } catch {}
  }

  const m = await ch.send({ embeds: [embed] });
  await oppsTable.update(oppRecordId, { [F.OPP_QUOTES_MESSAGE_ID]: String(m.id) });
  return m.id;
}


async function postConfirmedBulksSummary({ guild, oppRecordId, oppFields, totalPairs, sizeTotalsText, currency }) {
  if (!CONFIRMED_BULKS_CHANNEL_ID) return;

  const ch = await guild.channels.fetch(String(CONFIRMED_BULKS_CHANNEL_ID)).catch(() => null);
  if (!ch || !ch.isTextBased()) return;

  const oppId = asText(oppFields["Opportunity ID"]) || oppRecordId;
  const bulkId = toBulkId(oppId);

  const product = asText(oppFields[F.OPP_PRODUCT_NAME]) || "Bulk Opportunity";

  const sellUnit = formatMoney(currency, oppFields[F.OPP_FINAL_SELL_PRICE]);
  const discount = formatPercent(oppFields[F.OPP_FINAL_DISCOUNT_PCT]);

  const sellUnitRaw = Number(asText(oppFields[F.OPP_FINAL_SELL_PRICE]));
  const totalSell = Number.isFinite(sellUnitRaw) ? sellUnitRaw * totalPairs : null;
  const totalSellStr = totalSell === null ? "—" : formatMoney(currency, totalSell);

  const embed = new EmbedBuilder()
    .setTitle("✅ CONFIRMED BULK (SELL)")
    .setDescription(`**${bulkId}** — ${product}`)
    .setColor(0xffd300)
    .addFields(
      { name: "Final Total Pairs", value: `**${totalPairs}**`, inline: true },
      { name: "Final Sell Price", value: sellUnit, inline: true },
      { name: "Total Sell", value: totalSellStr, inline: true },
      { name: "Final Discount", value: discount, inline: true },
      { name: "Sizes", value: sizeTotalsText || "(none)", inline: false }
    );

  await ch.send({ embeds: [embed] });
}


/* =========================
   INTERACTIONS
========================= */

client.on(Events.InteractionCreate, async (interaction) => {
  const inGuild = !!interaction.guildId;

  // Staff confirm deposit button
  if (interaction.isButton() && interaction.customId.startsWith("deposit_confirm:")) {
    const commitmentId = interaction.customId.split("deposit_confirm:")[1];

    if (!interaction.guildId) {
      await interaction.reply({ content: "This button can only be used in the server.", flags: MessageFlags.Ephemeral });
      return;
    }

    const member = await interaction.guild.members.fetch(interaction.user.id).catch(() => null);
    if (!isStaffMember(member)) {
      await interaction.reply({ content: "⛔ Staff only.", flags: MessageFlags.Ephemeral });
      return;
    }

    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const c = await commitmentsTable.find(commitmentId);
    const depositPct = await getBuyerDepositPct(c.fields);

    const newStatus = depositPct >= 100 ? "Paid" : "Deposit Paid";
    const label = newStatus === "Paid" ? "Paid ✓" : "Deposit Paid ✓";
    await commitmentsTable.update(commitmentId, { [F.COM_STATUS]: newStatus });

    // Disable button
    try {
      const embed0 = interaction.message.embeds?.[0];
      const newEmbed = embed0 ? EmbedBuilder.from(embed0).setFooter({ text: "✅ Deposit confirmed by staff" }) : null;
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId(`deposit_confirm:${commitmentId}`).setLabel("Deposit Paid ✓").setStyle(ButtonStyle.Success).setDisabled(true)
      );
      await interaction.message.edit({ embeds: newEmbed ? [newEmbed] : [], components: [row] });
    } catch {}

    await interaction.editReply("✅ Deposit marked as paid.");
    return;
  }

    // Staff confirm remaining paid button
  if (interaction.isButton() && interaction.customId.startsWith("paid_confirm:")) {
    const commitmentId = interaction.customId.split("paid_confirm:")[1];

    if (!interaction.guildId) {
      await interaction.reply({
        content: "This button can only be used in the server.",
        flags: MessageFlags.Ephemeral,
      });
      return;
    }

    const member = await interaction.guild.members.fetch(interaction.user.id).catch(() => null);
    if (!isStaffMember(member)) {
      await interaction.reply({ content: "⛔ Staff only.", flags: MessageFlags.Ephemeral });
      return;
    }

    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    await commitmentsTable.update(commitmentId, { [F.COM_STATUS]: "Paid" });

    // Disable buttons on the message
    try {
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`deposit_confirm:${commitmentId}`)
          .setLabel("Deposit / Paid ✓")
          .setStyle(ButtonStyle.Secondary)
          .setDisabled(true),
        new ButtonBuilder()
          .setCustomId(`paid_confirm:${commitmentId}`)
          .setLabel("Paid ✓")
          .setStyle(ButtonStyle.Success)
          .setDisabled(true)
      );
      await interaction.message.edit({ components: [row] });
    } catch {}

    await interaction.editReply("✅ Marked as Paid.");
    return;
  }

  // Join Bulk
  if (interaction.isButton() && interaction.customId.startsWith("opp_join:")) {
    const opportunityRecordId = interaction.customId.split("opp_join:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const opp = await oppsTable.find(opportunityRecordId);
      const oppFields = opp.fields || {};

      const oppStatus = asText(oppFields[F.OPP_STATUS]) || "";
      if (oppStatus && oppStatus !== "Open") {
        await interaction.editReply("⛔ This bulk is closed.");
        return;
      }

      const buyer = await upsertBuyer(interaction.user);

      let commitment = await findLatestCommitment(interaction.user.id, opportunityRecordId);
      if (!commitment) {
        commitment = await createCommitment({
          oppRecordId: opportunityRecordId,
          buyerRecordId: buyer.id,
          discordId: interaction.user.id,
          discordTag: interaction.user.tag,
        });
      }

      const fresh = await commitmentsTable.find(commitment.id);
      const status = asText(fresh.fields[F.COM_STATUS]) || "Draft";

      const dm = await interaction.user.createDM();

      const oppEmbed = buildOpportunityEmbed(oppFields);
      const linesText = await getCartLinesText(commitment.id);
      const lastAction = asText(fresh.fields[F.COM_LAST_ACTION]);
      const cartEmbed = buildCartEmbed(linesText, status, lastAction);

      const sizes = await resolveAllowedSizesAndMaybeWriteback(opportunityRecordId, oppFields);
      const components = sizes.length ? buildSizeButtons(opportunityRecordId, sizes, status) : [];

      const dmChannelId = asText(fresh.fields[F.COM_DM_CHANNEL_ID]);
      const dmMessageId = asText(fresh.fields[F.COM_DM_MESSAGE_ID]);

      let panelMsg;
      if (dmChannelId && dmMessageId) {
        try {
          const ch = await client.channels.fetch(dmChannelId);
          panelMsg = await ch.messages.fetch(dmMessageId);
          await panelMsg.edit({ embeds: [oppEmbed, cartEmbed], components: components.length ? components : [] });
        } catch {
          panelMsg = await dm.send({ embeds: [oppEmbed, cartEmbed], components: components.length ? components : undefined });
        }
      } else {
        panelMsg = await dm.send({ embeds: [oppEmbed, cartEmbed], components: components.length ? components : undefined });
      }

      await updateCommitmentDM(commitment.id, dm.id, panelMsg.id);

      await interaction.editReply("✅ I’ve sent you a DM to build your cart.");
      return;
    } catch (err) {
      console.error("opp_join error:", err);
      await interaction.editReply("⚠️ I couldn’t DM you. Please enable DMs for this server and try again.");
      return;
    }
  }

  // Add More
  if (interaction.isButton() && interaction.customId.startsWith("cart_addmore:")) {
    const oppRecordId = interaction.customId.split("cart_addmore:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
      if (!commitment) return void (await interaction.editReply("🧾 No cart found yet."));

      const status = await getCommitmentStatus(commitment.id);
      if (status !== "Submitted") return void (await interaction.editReply("⚠️ Add More is only available after you submit."));

      await touchCommitment(commitment.id, { [F.COM_STATUS]: "Editing", [F.COM_LAST_ACTION]: "Editing" });
      await refreshDmPanel(oppRecordId, commitment.id);

      // Avoid DM clutter
      if (!interaction.guildId) {
        await interaction.deleteReply().catch(() => {});
        return;
      }
      await interaction.editReply("✅ Editing enabled. Add more sizes and press Submit again to confirm.");
      return;
    } catch (err) {
      console.error("cart_addmore error:", err);
      await interaction.editReply("⚠️ Could not enable editing.");
      return;
    }
  }

  // Locked indicator
  if (interaction.isButton() && interaction.customId.startsWith("cart_locked:")) {
    await interaction.reply({ content: "🔒 This commitment is locked. Contact staff if you need changes.", ...deferEphemeralIfGuild(inGuild) });
    return;
  }

  // Size pick -> modal
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

    const modal = new ModalBuilder().setCustomId(`qty_modal:${oppRecordId}:${encodedSize}`).setTitle(`Quantity for ${size}`);
    const qtyInput = new TextInputBuilder().setCustomId("qty").setLabel("Quantity (0 to remove)").setStyle(TextInputStyle.Short).setRequired(true);
    modal.addComponents(new ActionRowBuilder().addComponents(qtyInput));
    await interaction.showModal(modal);
    return;
  }

  // Modal submit
  if (interaction.isModalSubmit() && interaction.customId.startsWith("qty_modal:")) {
    await interaction.deferReply();

    const [, oppRecordId, encodedSize] = interaction.customId.split(":");
    const size = sizeKeyDecode(encodedSize);

    const qtyRaw = interaction.fields.getTextInputValue("qty");
    const qty = Number.parseInt(qtyRaw, 10);

    if (!Number.isFinite(qty) || qty < 0 || qty > 999) {
      await interaction.editReply({ content: "⚠️ Please enter a valid quantity (0–999)." });
      scheduleDeleteInteractionReply(interaction);
      return;
    }

    let commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
    if (!commitment) {
      const buyer = await upsertBuyer(interaction.user);
      commitment = await createCommitment({
        oppRecordId: oppRecordId,
        buyerRecordId: buyer.id,
        discordId: interaction.user.id,
        discordTag: interaction.user.tag,
      });
    }

    const statusNow = await getCommitmentStatus(commitment.id);
    if (!EDITABLE_STATUSES.has(statusNow)) {
      await interaction.editReply({ content: "⚠️ This commitment is not editable right now." });
      scheduleDeleteInteractionReply(interaction);
      return;
    }

    if (statusNow === "Editing") {
      const existingQty = await getLineQty(commitment.id, size);
      if (qty < existingQty) {
        await interaction.editReply({ content: "⚠️ While editing after submission, you can only **increase** quantities." });
        scheduleDeleteInteractionReply(interaction);
        return;
      }
    }

    await upsertLine(commitment.id, size, qty);
    await touchCommitment(commitment.id, { [F.COM_LAST_ACTION]: `Saved ${size} × ${qty}` });

    // Editing/Draft does NOT affect totals until Submit snapshots counted qty.
    await refreshDmPanel(oppRecordId, commitment.id);

    await interaction.deleteReply().catch(() => {});
    return;
  }

  // Clear cart (Draft only)
  if (interaction.isButton() && interaction.customId.startsWith("cart_clear:")) {
    const oppRecordId = interaction.customId.split("cart_clear:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
      if (!commitment) return void (await interaction.editReply("🧾 No cart found yet."));

      const status = await getCommitmentStatus(commitment.id);
      if (status !== "Draft") return void (await interaction.editReply("⚠️ You can only clear while in Draft."));

      await deleteAllLines(commitment.id);
      await touchCommitment(commitment.id, { [F.COM_STATUS]: "Draft", [F.COM_LAST_ACTION]: "Cleared cart" });
      await refreshDmPanel(oppRecordId, commitment.id);

      // Avoid DM clutter
      if (!interaction.guildId) {
        await interaction.deleteReply().catch(() => {});
        return;
      }
      await interaction.editReply("🧹 Cleared.");
      return;
    } catch (err) {
      console.error("cart_clear error:", err);
      await interaction.editReply("⚠️ Could not clear.");
      return;
    }
  }

  // Submit cart (Draft/Editing -> Submitted)
  if (interaction.isButton() && interaction.customId.startsWith("cart_submit:")) {
    const oppRecordId = interaction.customId.split("cart_submit:")[1];
    await interaction.deferReply(deferEphemeralIfGuild(inGuild));

    try {
      const commitment = await findLatestCommitment(interaction.user.id, oppRecordId);
      if (!commitment) return void (await interaction.editReply("🧾 No cart found yet."));

      const status = await getCommitmentStatus(commitment.id);
      if (status !== "Draft" && status !== "Editing") return void (await interaction.editReply("⚠️ This commitment can’t be submitted right now."));

      const cartText = await getCartLinesText(commitment.id);
      if (cartText.includes("No sizes selected")) return void (await interaction.editReply("⚠️ Your cart is empty."));

      await touchCommitment(commitment.id, {
        [F.COM_STATUS]: "Submitted",
        [F.COM_COMMITTED_AT]: new Date().toISOString(),
        [F.COM_LAST_ACTION]: "Submitted",
      });

      // snapshot counted qty so totals/tiers reflect last submitted state
      await snapshotCountedQuantities(commitment.id);

      // update totals/tiers
      await recalcOpportunityTotals(oppRecordId);
      await refreshDmPanel(oppRecordId, commitment.id);

      // Avoid DM clutter: delete interaction reply in DMs (panel shows status/last update)
      if (!interaction.guildId) {
        await interaction.deleteReply().catch(() => {});
        return;
      }
      await interaction.editReply("✅ Submitted.");
      return;
    } catch (err) {
      console.error("cart_submit error:", err);
      await interaction.editReply("⚠️ Could not submit.");
      return;
    }
  }
});

/* =========================
   EXPRESS API
========================= */

const app = express();
app.use(morgan("tiny"));
app.use(express.json());

function assertSecret(req, res) {
  const incomingSecret = req.header("x-post-secret") || "";
  if (!POST_OPP_SECRET || incomingSecret !== POST_OPP_SECRET) {
    res.status(401).json({ ok: false, error: "Unauthorized" });
    return false;
  }
  return true;
}

async function syncPublic(opportunityRecordId) {
  let opp = await oppsTable.find(opportunityRecordId);
  let fields = opp.fields || {};

  // ensure next tier fields exist even at 0
  const totalPairs0 = Number(fields[F.OPP_CURRENT_TOTAL_PAIRS] || 0) || 0;
  await recalcOpportunityPricing(opportunityRecordId, totalPairs0);
  opp = await oppsTable.find(opportunityRecordId);
  fields = opp.fields || {};

  const channelId = fields["Discord Public Channel ID"];
  const messageId = fields["Discord Public Message ID"];
  if (!channelId || !messageId) return;

  const channel = await client.channels.fetch(String(channelId));
  if (!channel || !channel.isTextBased()) return;

  const message = await channel.messages.fetch(String(messageId));
  const embed = buildOpportunityEmbed(fields);

  const oppStatus = asText(fields[F.OPP_STATUS]) || "";
  const disabled = oppStatus && oppStatus !== "Open";
  const row = buildJoinRow(opportunityRecordId, disabled, disabled ? "Closed" : "Join Bulk");

  await message.edit({ embeds: [embed], components: [row] });
}

async function syncDms(opportunityRecordId) {
  const rows = await commitmentsTable
    .select({
      maxRecords: 1000,
      filterByFormula: `AND({${F.COM_OPP_RECORD_ID}}='${escapeForFormula(opportunityRecordId)}',{${F.COM_DM_CHANNEL_ID}}!='',{${F.COM_DM_MESSAGE_ID}}!='')`,
    })
    .all();

  for (const c of rows) {
    try {
      await refreshDmPanel(opportunityRecordId, c.id);
    } catch (e) {
      console.warn("DM sync failed", c.id, e?.message || e);
    }
  }
}

app.get("/", (_req, res) => res.send("Bulk bot is live ✅"));

app.get("/airtable-test", async (_req, res) => {
  try {
    const records = await buyersTable.select({ maxRecords: 1 }).firstPage();
    res.json({ ok: true, buyers_records_found: records.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post("/post-opportunity", async (req, res) => {
  try {
    if (!assertSecret(req, res)) return;

    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });

    let opp = await oppsTable.find(opportunityRecordId);
    let fields = opp.fields || {};

    const totalPairs0 = Number(fields[F.OPP_CURRENT_TOTAL_PAIRS] || 0) || 0;
    await recalcOpportunityPricing(opportunityRecordId, totalPairs0);
    opp = await oppsTable.find(opportunityRecordId);
    fields = opp.fields || {};

    if (fields["Discord Public Message ID"]) {
      return res.json({ ok: true, skipped: true, reason: "Already posted", messageId: fields["Discord Public Message ID"] });
    }

    const embed = buildOpportunityEmbed(fields);
    const row = buildJoinRow(opportunityRecordId, false, "Join Bulk");

    const channel = await client.channels.fetch(String(BULK_PUBLIC_CHANNEL_ID));
    if (!channel || !channel.isTextBased()) return res.status(500).json({ ok: false, error: "Public channel not found" });

    const msg = await channel.send({ embeds: [embed], components: [row] });

    await oppsTable.update(opportunityRecordId, {
      "Discord Public Channel ID": String(BULK_PUBLIC_CHANNEL_ID),
      "Discord Public Message ID": String(msg.id),
      "Post Now": false,
    });

    return res.json({ ok: true, posted: true, messageId: msg.id });
  } catch (err) {
    console.error("post-opportunity error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post("/sync-opportunity", async (req, res) => {
  try {
    if (!assertSecret(req, res)) return;
    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });

    await syncPublic(opportunityRecordId);
    return res.json({ ok: true, synced: true });
  } catch (err) {
    console.error("sync-opportunity error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post("/sync-opportunity-dms", async (req, res) => {
  try {
    if (!assertSecret(req, res)) return;
    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });

    await syncDms(opportunityRecordId);
    return res.json({ ok: true, synced: true });
  } catch (err) {
    console.error("sync-opportunity-dms error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post("/close-opportunity", async (req, res) => {
  try {
    if (!assertSecret(req, res)) return;
    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });

    const guildId = await inferGuildId();
    if (!guildId) return res.status(500).json({ ok: false, error: "Could not infer guild id" });

    const guild = await client.guilds.fetch(guildId);

    const opp = await oppsTable.find(opportunityRecordId);
    const oppFields = opp.fields || {};

    await oppsTable.update(opportunityRecordId, { [F.OPP_STATUS]: "Closed" });

    const finalUnit = parseMoneyNumber(
      oppFields[F.OPP_CURRENT_SELL_PRICE] ?? oppFields[F.OPP_START_SELL_PRICE]
    );

    await setFinalUnitPriceForOpportunityCommitments(opportunityRecordId, finalUnit);

    const category = await ensureOppCategory(guild, opportunityRecordId, oppFields);
    const staffRoleIds = parseCsvIds(STAFF_ROLE_IDS);

    const commitments = await commitmentsTable
      .select({
        maxRecords: 1000,
        filterByFormula: `{${F.COM_OPP_RECORD_ID}}='${escapeForFormula(opportunityRecordId)}'`,
      })
      .all();

    const depositDueAt = asText(oppFields[F.OPP_DEPOSIT_DUE_AT]);

    let locked = 0;
    let cancelled = 0;
    const cancelledIds = new Set();
    let channelsCreated = 0;

    for (const c of commitments) {
      const st = asText(c.fields[F.COM_STATUS]) || "";

      if (st === "Draft") {
        await commitmentsTable.update(c.id, { [F.COM_STATUS]: "Cancelled" });
        cancelledIds.add(c.id);
        cancelled++;
        continue;
      }

      if (st === "Submitted" || st === "Editing") {
        await commitmentsTable.update(c.id, { [F.COM_STATUS]: "Locked" });
      }

      const newStatus = st === "Submitted" || st === "Editing" ? "Locked" : st;
      if (newStatus === "Locked") locked++;
      if (newStatus !== "Locked") continue;

      const buyerDiscordId = asText(c.fields[F.COM_DISCORD_USER_ID]);
      const buyerTag = asText(c.fields[F.COM_DISCORD_USER_TAG]) || buyerDiscordId;
      if (!buyerDiscordId) continue;

      let dealChannel = null;
      const existingDealChannelId = asText(c.fields[F.COM_DEAL_CHANNEL_ID]);
      if (existingDealChannelId) {
        dealChannel = await guild.channels.fetch(existingDealChannelId).catch(() => null);
      }

      const suffix = getCommitmentIdSuffix(c.fields) || opportunityRecordId.slice(-4);

      dealChannel = await ensureDealChannel({
        guild,
        categoryId: category.id,
        buyerDiscordId,
        buyerTag,
        staffRoleIds,
        nameSuffix: suffix,
      });
        await commitmentsTable.update(c.id, { [F.COM_DEAL_CHANNEL_ID]: String(dealChannel.id) });
        channelsCreated++;
      }

      const depositPct = await getBuyerDepositPct(c.fields);
      const totals = await computeCommitmentTotals(c.id, oppFields);
      const commitmentLinesText = await getCartLinesText(c.id);

      const embed = buildDepositEmbed({
        oppFields,
        commitmentLinesText,
        currency: totals.currency,
        unitPrice: totals.unitPrice,
        totalAmount: totals.totalAmount,
        depositPct,
        depositDueAt,
      });

      const components = depositPct > 0 ? [buildDepositButtonRow(c.id, true)] : [];

      const existingDealMsgId = asText(c.fields[F.COM_DEAL_MESSAGE_ID]);
      if (existingDealMsgId) {
        try {
          const m = await dealChannel.messages.fetch(existingDealMsgId);
          await m.edit({ embeds: [embed], components });
          continue;
        } catch {}
      }

      const m = await dealChannel.send({ embeds: [embed], components });
      await commitmentsTable.update(c.id, { [F.COM_DEAL_MESSAGE_ID]: String(m.id) });
    }

    // auto-sync public + DMs to lock buttons visually
    await syncPublic(opportunityRecordId);
    await syncDms(opportunityRecordId);

    return res.json({ ok: true, closed: true, locked, cancelled, channelsCreated });
  } catch (err) {
    console.error("close-opportunity error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

/* =========================
   CONFIRMED BULKS HELPERS
========================= */

async function findConfirmedBulkByOpportunity(opportunityRecordId) {
  const rows = await confirmedBulksTable
    .select({
      maxRecords: 1,
      // Works for linked-record fields that store an array of record IDs
      filterByFormula: `FIND('${escapeForFormula(
        opportunityRecordId
      )}', ARRAYJOIN({${F.CB_LINKED_OPPORTUNITY}}, ',')) > 0`,
    })
    .firstPage();

  return rows.length ? rows[0] : null;
}

async function createConfirmedBulkLinks({
  opportunityRecordId,
  eligibleCommitmentRecords, // Airtable commitment records (full records, not only IDs)
  oppFields, // pass the opportunity fields so we can link supplier too
}) {
  const linkedCommitmentIds = eligibleCommitmentRecords.map((r) => r.id);

  // Collect Buyer record IDs (linked field on each commitment)
  const buyerIds = new Set();
  for (const r of eligibleCommitmentRecords) {
    const links = r.fields?.[F.COM_BUYER];
    if (Array.isArray(links)) {
      for (const id of links) buyerIds.add(id);
    }
  }

  // Link Supplier (linked field on Opportunity)
  const supplierLinks = oppFields?.[F.OPP_SUPPLIER_LINK]; // should be an array of supplier record IDs
  const supplierId = Array.isArray(supplierLinks) ? supplierLinks[0] : null;

  const payload = {
    [F.CB_LINKED_OPPORTUNITY]: [opportunityRecordId],
    [F.CB_LINKED_COMMITMENTS]: linkedCommitmentIds,
    [F.CB_LINKED_BUYERS]: Array.from(buyerIds),
    ...(supplierId ? { [F.CB_LINKED_SUPPLIER]: [supplierId] } : {}),
  };

  const existing = await findConfirmedBulkByOpportunity(opportunityRecordId);
  if (existing) {
    return await confirmedBulksTable.update(existing.id, payload);
  }
  return await confirmedBulksTable.create(payload);
}

app.post("/finalize-opportunity", async (req, res) => {
  try {
    if (!assertSecret(req, res)) return;

    const { opportunityRecordId } = req.body || {};
    if (!opportunityRecordId) {
      return res.status(400).json({ ok: false, error: "opportunityRecordId is required" });
    }

    const guildId = await inferGuildId();
    if (!guildId) {
      return res.status(500).json({ ok: false, error: "Could not infer guild id" });
    }
    const guild = await client.guilds.fetch(guildId);

    // ---- Load opportunity ----
    let opp = await oppsTable.find(opportunityRecordId);
    let oppFields = opp.fields || {};

    // ---- Load all commitments for this opportunity ----
    const commitments = await commitmentsTable
      .select({
        maxRecords: 1000,
        filterByFormula: `{${F.COM_OPP_RECORD_ID}}='${escapeForFormula(opportunityRecordId)}'`,
      })
      .all();

    // ---- Determine eligible commitments ----
    const eligibleCommitmentIds = [];
    let cancelled = 0;

    for (const c of commitments) {
      const st = asText(c.fields[F.COM_STATUS]) || "";
      const depositPct = await getBuyerDepositPct(c.fields);

      const isEligible = st === "Paid" || st === "Deposit Paid" || (depositPct <= 0 && st === "Locked");

      if (isEligible) {
        eligibleCommitmentIds.push(c.id);
      } else if (st === "Locked" && depositPct > 0) {
        await commitmentsTable.update(c.id, { [F.COM_STATUS]: "Cancelled" });
        cancelled++;
      }
    }

    // ---- Build size totals from eligible commitments using Counted Quantity ----
    const sizeTotals = new Map();

    for (let i = 0; i < eligibleCommitmentIds.length; i += 25) {
      const chunk = eligibleCommitmentIds.slice(i, i + 25);
      const orChunk = buildOrFormula(F.LINE_COMMITMENT_RECORD_ID, chunk);

      const lines = await linesTable
        .select({
          filterByFormula: `${orChunk}`,
          maxRecords: 1000,
        })
        .all();

      for (const line of lines) {
        const size = asText(line.fields[F.LINE_SIZE]);
        const qty = Number(line.fields?.[F.LINE_COUNTED_QTY] ?? 0);
        if (!size || !Number.isFinite(qty) || qty <= 0) continue;
        sizeTotals.set(size, (sizeTotals.get(size) || 0) + qty);
      }
    }

    const sortedSizes = Array.from(sizeTotals.entries()).sort((a, b) => a[0].localeCompare(b[0]));
    const sizeTotalsText = sortedSizes.length ? sortedSizes.map(([s, q]) => `• ${s}: ${q}`).join(NL) : "(none)";
    const totalPairs = sortedSizes.reduce((sum, [, q]) => sum + q, 0);
    const currency = asText(oppFields[F.OPP_CURRENCY]) || "EUR";

    // ---- Snapshot final fields on opportunity ----
    const finalSellPrice = parseMoneyNumber(
      oppFields[F.OPP_CURRENT_SELL_PRICE] ?? oppFields[F.OPP_START_SELL_PRICE]
    );

    await oppsTable.update(opportunityRecordId, {
      [F.OPP_FINAL_TOTAL_PAIRS]: totalPairs,
      [F.OPP_FINAL_SELL_PRICE]: Number.isFinite(finalSellPrice) ? finalSellPrice : null,
      [F.OPP_FINAL_DISCOUNT_PCT]: oppFields[F.OPP_CURRENT_DISCOUNT] ?? 0,
      [F.OPP_STATUS]: "Confirmed",
    });

    // Re-fetch to ensure Final fields exist for summaries + confirmed bulks record
    opp = await oppsTable.find(opportunityRecordId);
    oppFields = opp.fields || {};

    // ---- Post supplier quote (BUY) + confirmed bulks summary (SELL) ----
    await postSupplierQuote({ guild, oppRecordId: opportunityRecordId, oppFields, sizeTotalsText, totalPairs, currency });
    await postConfirmedBulksSummary({ guild, oppRecordId: opportunityRecordId, oppFields, totalPairs, sizeTotalsText, currency });

    // ---- Create/Update Confirmed Bulks record (LINKS only) ----
    const eligibleRecords = commitments.filter((c) => eligibleCommitmentIds.includes(c.id));
    if (eligibleRecords.length) {
      try {
        await createConfirmedBulkLinks({
          opportunityRecordId,
          eligibleCommitmentRecords: eligibleRecords,
          oppFields, // extra arg is ok even if function ignores it
        });
      } catch (e) {
        console.warn("⚠️ Confirmed Bulks linking failed:", e?.message || e);
      }
    }

    // ---- Notify buyers + edit deal message buttons ----
    for (const c of commitments) {
      const dealChannelId = asText(c.fields[F.COM_DEAL_CHANNEL_ID]);
      if (!dealChannelId) continue;

      let freshC;
      try {
        freshC = await commitmentsTable.find(c.id);
      } catch {
        freshC = c;
      }

      const stFresh = asText(freshC.fields[F.COM_STATUS]) || "";
      const depositPctFresh = await getBuyerDepositPct(freshC.fields);

      const eligibleFresh =
        stFresh === "Paid" || stFresh === "Deposit Paid" || (depositPctFresh <= 0 && stFresh === "Locked");

      try {
        const ch = await guild.channels.fetch(dealChannelId);
        if (!ch || !ch.isTextBased()) continue;

        const dealMsgId = asText(freshC.fields[F.COM_DEAL_MESSAGE_ID]);

        // Eligible: message + "Confirm Remaining Paid" button if not Paid yet
        if (eligibleFresh) {
          await ch.send("✅ Included in supplier order. We will update you here once we have tracking / ETA.");

          if (dealMsgId && stFresh !== "Paid") {
            try {
              const m = await ch.messages.fetch(dealMsgId);

              const row = new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                  .setCustomId(`deposit_confirm:${freshC.id}`)
                  .setLabel("Deposit / Paid ✓")
                  .setStyle(ButtonStyle.Secondary)
                  .setDisabled(true),
                new ButtonBuilder()
                  .setCustomId(`paid_confirm:${freshC.id}`)
                  .setLabel("Confirm Remaining Paid")
                  .setStyle(ButtonStyle.Success)
              );

              await m.edit({ components: [row] });
            } catch (_) {}
          }

          continue;
        }

        // Cancelled: message + disable buttons
        if (stFresh === "Cancelled") {
          await ch.send("❌ Deposit not received in time. Your commitment has been cancelled.");

          if (dealMsgId) {
            try {
              const m = await ch.messages.fetch(dealMsgId);

              const row = new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                  .setCustomId(`deposit_confirm:${freshC.id}`)
                  .setLabel("Cancelled")
                  .setStyle(ButtonStyle.Danger)
                  .setDisabled(true)
              );

              await m.edit({ components: [row] });
            } catch (_) {}
          }
        }
      } catch (_) {}
    }

    // ---- auto-sync public + DMs (locks buttons visually) ----
    await syncPublic(opportunityRecordId);
    await syncDms(opportunityRecordId);

    return res.json({
      ok: true,
      finalized: true,
      eligible: eligibleCommitmentIds.length,
      cancelled,
      totalPairs,
    });
  } catch (err) {
    console.error("finalize-opportunity error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.listen(LISTEN_PORT, () => console.log(`🌐 Listening on ${LISTEN_PORT}`));

client.login(DISCORD_TOKEN);
