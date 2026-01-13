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
  AIRTABLE_SIZE_PRESETS_TABLE = "Size Presets",


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

console.log("✅ Airtable base configured:", AIRTABLE_BASE_ID);
console.log("✅ Buyers table:", AIRTABLE_BUYERS_TABLE);
console.log("✅ Opportunities table:", AIRTABLE_OPPS_TABLE);
console.log("✅ Commitments table:", AIRTABLE_COMMITMENTS_TABLE);
console.log("✅ Commitment Lines table:", AIRTABLE_LINES_TABLE);
console.log("✅ Size Presets table:", AIRTABLE_SIZE_PRESETS_TABLE);

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
  OPP_BRAND: "Brand",
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
  COM_OPP_RECORD_ID: "Opportunity Record ID", // optional, but nice

  // Lines
  LINE_COMMITMENT: "Commitment",
  LINE_COMMITMENT_RECORD_ID: "Commitment Record ID",
  LINE_SIZE: "Size",
  LINE_QTY: "Quantity",

  // Size Presets
  PRESET_BRAND: "Brand",
  PRESET_SIZE_LADDER: "Size Ladder",
  PRESET_LINKED_SKUS: "Linked SKU's",
  
};

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
  const raw = asText(v);
  if (raw === "") return "—";
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

function parseSizeList(v) {
  const raw = asText(v);
  if (!raw) return [];
  return raw.split(",").map((s) => s.trim()).filter(Boolean);
}

const ladderCache = new Map(); // key -> array of sizes

function parseLadder(v) {
  // Airtable in your screenshot uses comma-separated values
  return asText(v)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function normalizeSku(s) {
  return String(s || "").trim().toUpperCase();
}
function normalizeBrand(s) {
  return String(s || "").trim().toLowerCase();
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

async function resolveAllowedSizesAndMaybeWriteback(oppRecordId, oppFields) {
  // 1) if already filled, use it
  const existing = parseSizeList(oppFields[F.OPP_ALLOWED_SIZES]);
  if (existing.length) return existing;

  // 2) build from presets by SKU
  const sku = asText(oppFields[F.OPP_SKU_SOFT]) || asText(oppFields[F.OPP_SKU]);
  const ladder = await getPresetLadderBySku(sku);
  if (!ladder.length) return [];

  const sliced = sliceLadderByMinMax(ladder, oppFields[F.OPP_MIN_SIZE], oppFields[F.OPP_MAX_SIZE]);
  if (!sliced.length) return [];

  // 3) OPTIONAL writeback (fills on first Join Bulk)
  await oppsTable.update(oppRecordId, {
    [F.OPP_ALLOWED_SIZES]: sliced.join(", "),
  });

  return sliced;
}

function sizeKeyEncode(size) {
  // Make safe for customId
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

  const currentPrice = formatMoney(
    currency,
    fields[F.OPP_CURRENT_SELL_PRICE] ?? fields[F.OPP_START_SELL_PRICE]
  );

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
  ].join("\n");

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
    // Keep username fresh
    try {
      await buyersTable.update(existing[0].id, {
        [F.BUYER_DISCORD_USERNAME]: username,
      });
    } catch (_) {}
    return existing[0];
  }

  const created = await buyersTable.create({
    [F.BUYER_DISCORD_ID]: discordId,
    [F.BUYER_DISCORD_USERNAME]: username,
  });
  return created;
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
  };
  // optional helper field if you created it
  payload[F.COM_OPP_RECORD_ID] = oppRecordId;

  const created = await commitmentsTable.create(payload);
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

function buildSizeButtons(opportunityRecordId, sizes) {
  const rows = [];
  let row = new ActionRowBuilder();
  let inRow = 0;

  for (const s of sizes) {
    if (rows.length === 4) break; // leave 5th row for controls
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
    );
    inRow++;
  }
  if (inRow > 0 && rows.length < 4) rows.push(row);

  rows.push(
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`cart_review:${opportunityRecordId}`)
        .setLabel("Review")
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId(`cart_submit:${opportunityRecordId}`)
        .setLabel("Submit")
        .setStyle(ButtonStyle.Success),
      new ButtonBuilder()
        .setCustomId(`cart_clear:${opportunityRecordId}`)
        .setLabel("Clear")
        .setStyle(ButtonStyle.Danger)
    )
  );

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

client.on(Events.InteractionCreate, async (interaction) => {
  const inGuild = !!interaction.guildId;
  const ephemeral = inGuild;

  /* ---------- Join Bulk ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("opp_join:")) {
    const opportunityRecordId = interaction.customId.split("opp_join:")[1];
    await interaction.deferReply({ ephemeral });

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

      const dm = await interaction.user.createDM();

      const oppEmbed = buildOpportunityEmbed(oppFields);
      const cartEmbed = new EmbedBuilder()
        .setTitle("🧾 Bulk Cart")
        .setDescription(await getCartLinesText(commitment.id))
        .setColor(0xffd300);

      const sizes = await resolveAllowedSizesAndMaybeWriteback(opportunityRecordId, oppFields);
      const components =
        sizes.length > 0
          ? buildSizeButtons(opportunityRecordId, sizes)
          : [];

      const msg = await dm.send({
        embeds: [oppEmbed, cartEmbed],
        components: components.length ? components : undefined,
      });

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

  /* ---------- Size button -> modal ---------- */
  if (interaction.isButton() && interaction.customId.startsWith("size_pick:")) {
    const [, oppRecordId, encodedSize] = interaction.customId.split(":");
    const size = sizeKeyDecode(encodedSize);

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

  /* ---------- Modal submit -> upsert line + update DM panel ---------- */
  if (interaction.isModalSubmit() && interaction.customId.startsWith("qty_modal:")) {
    const [, oppRecordId, encodedSize] = interaction.customId.split(":");
    const size = sizeKeyDecode(encodedSize);

    const qtyRaw = interaction.fields.getTextInputValue("qty");
    const qty = Number.parseInt(qtyRaw, 10);

    if (!Number.isFinite(qty) || qty < 0 || qty > 999) {
      await interaction.reply({ content: "⚠️ Please enter a valid quantity (0–999).", ephemeral: true });
      return;
    }

    // Find commitment
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

    await upsertLine(commitment.id, size, qty);

    // Refresh opportunity + rebuild DM panel
    const opp = await oppsTable.find(oppRecordId);
    const oppFields = opp.fields || {};
    const oppEmbed = buildOpportunityEmbed(oppFields);

    // Re-fetch commitment to read stored DM ids
    const freshCommitment = await commitmentsTable.find(commitment.id);
    const dmChannelId = asText(freshCommitment.fields[F.COM_DM_CHANNEL_ID]);
    const dmMessageId = asText(freshCommitment.fields[F.COM_DM_MESSAGE_ID]);

    const cartEmbed = new EmbedBuilder()
      .setTitle("🧾 Bulk Cart")
      .setDescription(await getCartLinesText(commitment.id))
      .setColor(0xffd300);

    const sizes = await resolveAllowedSizesAndMaybeWriteback(oppRecordId, oppFields);
    const components =
      sizes.length > 0
        ? buildSizeButtons(oppRecordId, sizes)
        : [];

    if (dmChannelId && dmMessageId) {
      const ch = await client.channels.fetch(dmChannelId);
      const msg = await ch.messages.fetch(dmMessageId);
      await msg.edit({
        embeds: [oppEmbed, cartEmbed],
        components: components.length ? components : [],
      });
    }

    await interaction.reply({ content: `✅ Saved: **${size} × ${qty}**`, ephemeral: true });
    return;
  }

  // placeholders for next step (Review/Submit/Clear)
});

/* =========================
   Express (post + sync embeds)
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
      return res.json({ ok: true, skipped: true, reason: "Already posted", messageId: fields["Discord Public Message ID"] });
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
      return res.status(400).json({ ok: false, error: "Missing Discord Public Channel ID or Discord Public Message ID" });
    }

    const channel = await client.channels.fetch(String(channelId));
    if (!channel || !channel.isTextBased()) {
      return res.status(500).json({ ok: false, error: "Channel not found or not text-based" });
    }

    const message = await channel.messages.fetch(String(messageId));
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

client.login(DISCORD_TOKEN);
