import { EmbedBuilder, Events } from "discord.js";

export function registerLeaderboards(ctx) {
  const { client, base, env } = ctx;

  const {
    AFFILIATE_GUILD_ID,
    AIRTABLE_MEMBERS_TABLE = "Discord Members",
    AIRTABLE_INVITES_LOG_TABLE = "Invites Log",

    LEADERBOARD_CHANNEL_ID,
    WINNERS_CHANNEL_ID,

    LEADERBOARD_TOP_N = "10",
    REFERRAL_QUALIFIED_FIELD = "Referral Qualified",
    REFERRAL_FEE_EUR = "5",
  } = env;

  if (!LEADERBOARD_CHANNEL_ID || !WINNERS_CHANNEL_ID) {
    console.warn("⚠️ Leaderboards disabled: LEADERBOARD_CHANNEL_ID or WINNERS_CHANNEL_ID missing.");
    return;
  }

  const membersTable = base(AIRTABLE_MEMBERS_TABLE);
  const invitesLogTable = base(AIRTABLE_INVITES_LOG_TABLE);

  const TOP_N = Math.max(3, Math.min(25, parseInt(LEADERBOARD_TOP_N, 10) || 10));
  const FEE = Number(REFERRAL_FEE_EUR) || 5;

  // Cache Discord username lookups (Airtable)
  const nameCache = new Map(); // discordId -> username

  function escapeAirtableValue(v) {
    return String(v || "").replace(/'/g, "\\'");
  }
  function normId(v) {
    return String(v || "").trim();
  }

  // Amsterdam month key YYYY-MM
  function monthKeyAmsterdam(date = new Date()) {
    const fmt = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Amsterdam",
      year: "numeric",
      month: "2-digit",
    });
    // en-CA gives YYYY-MM
    return fmt.format(date);
  }

  function prevMonthKey(yyyyMm) {
    const [y, m] = yyyyMm.split("-").map((x) => parseInt(x, 10));
    const d = new Date(Date.UTC(y, m - 1, 1)); // month is 0-based internally
    d.setUTCMonth(d.getUTCMonth() - 1);
    const yy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    return `${yy}-${mm}`;
  }

  async function getDiscordUsername(discordId) {
    const id = normId(discordId);
    if (!id) return "Unknown";
    if (nameCache.has(id)) return nameCache.get(id);

    const rows = await membersTable
      .select({
        maxRecords: 1,
        filterByFormula: `{Discord User ID}='${escapeAirtableValue(id)}'`,
      })
      .firstPage()
      .catch(() => []);

    const username = rows?.[0]?.fields?.["Discord Username"] || `User ${id.slice(-4)}`;
    nameCache.set(id, username);
    return username;
  }

  async function fetchInviteRowsForMonth(monthKey) {
    // Invites Log -> all rows for a month
    const rows = await invitesLogTable
      .select({
        filterByFormula: `{Month Key}='${escapeAirtableValue(monthKey)}'`,
        fields: ["Inviter Discord User ID", REFERRAL_QUALIFIED_FIELD],
        pageSize: 100,
      })
      .all()
      .catch((e) => {
        console.error("LB: invitesLog select failed:", e);
        return [];
      });

    return rows || [];
  }

  async function findOrCreatePinnedLeaderboardMessage(channel) {
    const recent = await channel.messages.fetch({ limit: 50 }).catch(() => null);
    const existing = recent?.find(
      (m) =>
        m.author?.id === channel.client.user.id &&
        m.embeds?.[0]?.title?.startsWith("🏆 LEADERBOARD —")
    );

    if (existing) {
      if (!existing.pinned) await existing.pin().catch(() => {});
      return existing;
    }

    const msg = await channel.send({ content: "🏆 Leaderboard is initializing..." });
    await msg.pin().catch(() => {});
    return msg;
  }

  async function getMemberRecordByDiscordId(discordId) {
    const id = normId(discordId);
    const rows = await membersTable
      .select({
        maxRecords: 1,
        filterByFormula: `{Discord User ID}='${escapeAirtableValue(id)}'`,
        fields: ["Discord User ID", "Discord Username", "Last Earnings DM Month"],
      })
      .firstPage()
      .catch(() => []);

    return rows?.[0] || null;
  }

  async function sendMonthlyEarningsDMs(monthKey) {
    console.log("TEST: sendMonthlyEarningsDMs fired for", monthKey);

    const rows = await invitesLogTable
      .select({
        filterByFormula: `AND(
          {Month Key}='${escapeAirtableValue(monthKey)}',
          {${REFERRAL_QUALIFIED_FIELD}}=TRUE()
        )`,
        fields: ["Inviter Discord User ID"],
        pageSize: 100,
      })
      .all()
      .catch((e) => {
        console.error("LB: fetch qualified rows failed:", e);
        return [];
      });

    console.log("TEST: qualified rows found:", rows.length);

    const qualifiedCounts = new Map();
    for (const r of rows) {
      const inviterId = normId(r.fields?.["Inviter Discord User ID"]);
      if (!inviterId) continue;
      qualifiedCounts.set(inviterId, (qualifiedCounts.get(inviterId) || 0) + 1);
    }

    for (const [inviterId, q] of qualifiedCounts.entries()) {
      if (!q || q <= 0) continue;

      const memberRec = await getMemberRecordByDiscordId(inviterId);
      if (!memberRec) continue;

      const alreadyMonth = memberRec.fields?.["Last Earnings DM Month"];
      if (alreadyMonth === monthKey) continue;

      const eur = q * FEE;
      const username = memberRec.fields?.["Discord Username"] || inviterId;

      const user = await client.users.fetch(inviterId).catch(() => null);
      if (!user) continue;

      const embed = new EmbedBuilder()
        .setTitle(`💰 Affiliate Summary — ${monthKey}`)
        .setDescription(
          [
            `You earned **€${eur}** this month.`,
            "",
            `✅ **${q}** invited members completed their **first deal**.`,
            "",
            "Thanks for helping grow **Kickz Caviar** 🤝",
          ].join("\n")
        )
        .setColor(0xffd300)
        .setFooter({ text: "Kickz Caviar Affiliate Program" })
        .setTimestamp();

      const sentOk = await user.send({ embeds: [embed] })
        .then(() => true)
        .catch((e) => {
          console.error("LB: DM failed (DMs closed?)", inviterId, e?.message || e);
          return false;
        });

      if (sentOk) {
        await membersTable.update(memberRec.id, {
          "Last Earnings DM Month": monthKey,
        }).catch((e) => console.error("LB: failed to mark Last Earnings DM Month", e));

        console.log("LB: DM sent", username, "€", eur, "month", monthKey);
      }
    }
  }

  async function buildLeaderboardsForMonth(monthKey) {
    const rows = await fetchInviteRowsForMonth(monthKey);

    // counts
    const inviteCounts = new Map();     // inviterId -> count
    const qualifiedCounts = new Map();  // inviterId -> qualified count

    for (const r of rows) {
      const f = r.fields || {};
      const inviterId = normId(f["Inviter Discord User ID"]);
      if (!inviterId) continue;

      inviteCounts.set(inviterId, (inviteCounts.get(inviterId) || 0) + 1);

      const qualified = Boolean(f[REFERRAL_QUALIFIED_FIELD]);
      if (qualified) {
        qualifiedCounts.set(inviterId, (qualifiedCounts.get(inviterId) || 0) + 1);
      }
    }

    // sort helpers
    const topInvites = [...inviteCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, TOP_N);

    const topEarnings = [...qualifiedCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, TOP_N);

    // format lines with usernames
    const inviteLines = [];
    for (let i = 0; i < topInvites.length; i++) {
      const [id, c] = topInvites[i];
      const name = await getDiscordUsername(id);
      inviteLines.push(`${i + 1}. **${name}** — ${c}`);
    }
    if (!inviteLines.length) inviteLines.push("No invites yet this month.");

    const earnLines = [];
    for (let i = 0; i < topEarnings.length; i++) {
      const [id, q] = topEarnings[i];
      const name = await getDiscordUsername(id);
      const eur = q * FEE;
      earnLines.push(`${i + 1}. **${name}** — €${eur} (${q})`);
    }
    if (!earnLines.length) earnLines.push("No qualified referrals yet this month.");

    return { inviteLines, earnLines, monthKey };
  }

  async function findOrCreateLeaderboardMessage(channel) {
    const recent = await channel.messages.fetch({ limit: 10 }).catch(() => null);
    const existing = recent?.find(
      (m) =>
        m.author?.id === client.user.id &&
        m.embeds?.[0]?.title?.startsWith("🏆 LEADERBOARD —")
    );

    if (existing) return existing;

    return await channel.send({ content: "🏆 Leaderboard is initializing..." });
  }

  async function postWinnersIfNotPosted(winnersChannel, monthKey, embed) {
    // Avoid duplicates: if bot already posted "FINAL RESULTS — YYYY-MM" in last 50, skip
    const recent = await winnersChannel.messages.fetch({ limit: 50 }).catch(() => null);
    const already = recent?.some(
      (m) =>
        m.author?.id === client.user.id &&
        m.embeds?.[0]?.title === `🏁 FINAL RESULTS — ${monthKey}`
    );

    if (already) return;

    await winnersChannel.send({ embeds: [embed] }).catch((e) => {
      console.error("LB: posting winners failed:", e);
    });
  }

  async function renderCurrentMonthEmbed(monthKey, inviteLines, earnLines) {
    const now = new Date();
    const lastUpdated = new Intl.DateTimeFormat("nl-NL", {
      timeZone: "Europe/Amsterdam",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    }).format(now);

    return new EmbedBuilder()
      .setTitle(`🏆 LEADERBOARD — ${monthKey}`)
      .setDescription(`Last updated: **${lastUpdated}**`)
      .addFields(
        { name: "\u200B", value: "\u200B" }, // space after timestamp

        { name: "🔥 Top Inviters", value: inviteLines.join("\n") },

        { name: "\u200B", value: "\u200B" }, // space between leaderboards

        { name: `💰 Top Affiliates`, value: earnLines.join("\n") }
      );
  }

  async function renderFinalMonthEmbed(monthKey, inviteLines, earnLines) {
    return new EmbedBuilder()
      .setTitle(`🏁 FINAL RESULTS — ${monthKey}`)
      .setDescription("Locked statistics.")
      .addFields(
        { name: "\u200B", value: "\u200B" },

        { name: "🔥 Top Inviters", value: inviteLines.join("\n") },

        { name: "\u200B", value: "\u200B" },

        { name: `💰 Top Affiliates`, value: earnLines.join("\n") }
      );
  }

  let currentMonth = null;

  async function tick() {
    try {
      // Optional: only operate in one guild
      if (AFFILIATE_GUILD_ID) {
        const g = client.guilds.cache.get(String(AFFILIATE_GUILD_ID));
        if (!g) return;
      }
      
      const lbChannel = await client.channels.fetch(String(LEADERBOARD_CHANNEL_ID)).catch(() => null);
      const winnersChannel = await client.channels.fetch(String(WINNERS_CHANNEL_ID)).catch(() => null);
      if (!lbChannel?.isTextBased() || !winnersChannel?.isTextBased()) return;

      const nowMonth = monthKeyAmsterdam(new Date());

      // Month rollover: post final results for previous month (once)
      if (currentMonth && currentMonth !== nowMonth) {
        const prev = prevMonthKey(nowMonth);
        const prevData = await buildLeaderboardsForMonth(prev);
        const finalEmbed = await renderFinalMonthEmbed(prev, prevData.inviteLines, prevData.earnLines);
        await postWinnersIfNotPosted(winnersChannel, prev, finalEmbed); 
      }

      currentMonth = nowMonth;

      // Update pinned current month leaderboard
      const data = await buildLeaderboardsForMonth(nowMonth);
      const embed = await renderCurrentMonthEmbed(nowMonth, data.inviteLines, data.earnLines);

      const msg = await findOrCreatePinnedLeaderboardMessage(lbChannel);
      await msg.edit({ content: null, embeds: [embed] }).catch((e) => {
        console.error("LB: edit pinned leaderboard failed:", e);
      });
    } catch (e) {
      console.error("LB: tick error:", e);
    }
  }

  client.once(Events.ClientReady, async () => {
    console.log("✅ Leaderboards module ready.");

    console.log("TEST: calling sendMonthlyEarningsDMs(2026-01)");
    await sendMonthlyEarningsDMs("2026-01");
    console.log("TEST: done sendMonthlyEarningsDMs");

    await tick();
    setInterval(tick, 10 * 60 * 1000);
  });
}
