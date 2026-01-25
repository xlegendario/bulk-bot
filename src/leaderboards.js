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
    console.warn("⚠️ Leaderboards disabled: missing channel IDs.");
    return;
  }

  const membersTable = base(AIRTABLE_MEMBERS_TABLE);
  const invitesLogTable = base(AIRTABLE_INVITES_LOG_TABLE);

  const TOP_N = Math.max(3, Math.min(25, parseInt(LEADERBOARD_TOP_N, 10) || 10));
  const FEE = Number(REFERRAL_FEE_EUR) || 5;

  const nameCache = new Map(); // discordId -> username

  const escape = (v) => String(v || "").replace(/'/g, "\\'");
  const norm = (v) => String(v || "").trim();

  // YYYY-MM in Amsterdam time
  function monthKeyAmsterdam(date = new Date()) {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Amsterdam",
      year: "numeric",
      month: "2-digit",
    }).format(date);
  }

  function prevMonthKey(yyyyMm) {
    const [y, m] = yyyyMm.split("-").map(Number);
    const d = new Date(Date.UTC(y, m - 1, 1));
    d.setUTCMonth(d.getUTCMonth() - 1);
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
  }

  async function getDiscordUsername(discordId) {
    const id = norm(discordId);
    if (!id) return "Unknown";
    if (nameCache.has(id)) return nameCache.get(id);

    const rows = await membersTable
      .select({
        maxRecords: 1,
        filterByFormula: `{Discord User ID}='${escape(id)}'`,
      })
      .firstPage()
      .catch(() => []);

    const name = rows?.[0]?.fields?.["Discord Username"] || `User ${id.slice(-4)}`;
    nameCache.set(id, name);
    return name;
  }

  async function fetchInviteRowsForMonth(monthKey) {
    return (
      (await invitesLogTable
        .select({
          filterByFormula: `{Month Key}='${escape(monthKey)}'`,
          fields: ["Inviter Discord User ID", REFERRAL_QUALIFIED_FIELD],
        })
        .all()
        .catch(() => [])) || []
    );
  }

  async function findOrCreatePinnedLeaderboardMessage(channel) {
    const recent = await channel.messages.fetch({ limit: 50 }).catch(() => null);
    const existing = recent?.find(
      (m) =>
        m.author?.id === client.user.id &&
        m.embeds?.[0]?.title?.startsWith("🏆 LEADERBOARD —")
    );

    if (existing) {
      if (!existing.pinned) await existing.pin().catch(() => {});
      return existing;
    }

    const msg = await channel.send({ content: "🏆 Leaderboard initializing..." });
    await msg.pin().catch(() => {});
    return msg;
  }

  async function buildLeaderboardsForMonth(monthKey) {
    const rows = await fetchInviteRowsForMonth(monthKey);

    const inviteCounts = new Map();
    const qualifiedCounts = new Map();

    for (const r of rows) {
      const inviterId = norm(r.fields?.["Inviter Discord User ID"]);
      if (!inviterId) continue;

      inviteCounts.set(inviterId, (inviteCounts.get(inviterId) || 0) + 1);

      if (r.fields?.[REFERRAL_QUALIFIED_FIELD]) {
        qualifiedCounts.set(inviterId, (qualifiedCounts.get(inviterId) || 0) + 1);
      }
    }

    const topInvites = [...inviteCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, TOP_N);

    const topAffiliates = [...qualifiedCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, TOP_N);

    const inviteLines = [];
    for (let i = 0; i < topInvites.length; i++) {
      const [id, c] = topInvites[i];
      inviteLines.push(`${i + 1}. **${await getDiscordUsername(id)}** — ${c}`);
    }
    if (!inviteLines.length) inviteLines.push("No invites yet this month.");

    const affiliateLines = [];
    for (let i = 0; i < topAffiliates.length; i++) {
      const [id, q] = topAffiliates[i];
      affiliateLines.push(
        `${i + 1}. **${await getDiscordUsername(id)}** — €${q * FEE} (${q})`
      );
    }
    if (!affiliateLines.length) affiliateLines.push("No qualified referrals yet.");

    return { inviteLines, affiliateLines };
  }

  async function sendMonthlyEarningsDMs(monthKey) {
    const rows = await invitesLogTable
      .select({
        filterByFormula: `AND(
          {Month Key}='${escape(monthKey)}',
          {${escape(REFERRAL_QUALIFIED_FIELD)}}=TRUE()
        )`,
        fields: ["Inviter Discord User ID"],
      })
      .all()
      .catch(() => []);

    const counts = new Map();
    for (const r of rows) {
      const id = norm(r.fields?.["Inviter Discord User ID"]);
      if (!id) continue;
      counts.set(id, (counts.get(id) || 0) + 1);
    }

    for (const [inviterId, q] of counts.entries()) {
      const rec = (
        await membersTable
          .select({
            maxRecords: 1,
            filterByFormula: `{Discord User ID}='${escape(inviterId)}'`,
            fields: ["Last Earnings DM Month", "Discord Username"],
          })
          .firstPage()
          .catch(() => [])
      )?.[0];

      if (!rec || rec.fields?.["Last Earnings DM Month"] === monthKey) continue;

      const user = await client.users.fetch(inviterId).catch(() => null);
      if (!user) continue;

      const embed = new EmbedBuilder()
        .setTitle(`💰 Affiliate Summary — ${monthKey}`)
        .setColor(0x00c389)
        .setDescription(
          `You earned **€${q * FEE}** from **${q} qualified referrals**.\n\nThanks for helping grow Kickz Caviar 🤝`
        );

      const sent = await user.send({ embeds: [embed] }).then(() => true).catch(() => false);
      if (!sent) continue;

      await membersTable.update(rec.id, {
        "Last Earnings DM Month": monthKey,
      });
    }
  }

  let currentMonth = null;

  async function tick() {
    try {
      if (AFFILIATE_GUILD_ID && !client.guilds.cache.get(String(AFFILIATE_GUILD_ID))) return;

      const lbChannel = await client.channels.fetch(LEADERBOARD_CHANNEL_ID).catch(() => null);
      const winnersChannel = await client.channels.fetch(WINNERS_CHANNEL_ID).catch(() => null);
      if (!lbChannel?.isTextBased() || !winnersChannel?.isTextBased()) return;

      const nowMonth = monthKeyAmsterdam();

      if (currentMonth && currentMonth !== nowMonth) {
        const prev = prevMonthKey(nowMonth);
        const data = await buildLeaderboardsForMonth(prev);

        const finalEmbed = new EmbedBuilder()
          .setTitle(`🏁 FINAL RESULTS — ${prev}`)
          .addFields(
            { name: "🔥 Top Inviters", value: data.inviteLines.join("\n") },
            { name: "💰 Top Affiliates", value: data.affiliateLines.join("\n") }
          );

        await winnersChannel.send({ embeds: [finalEmbed] }).catch(() => {});
        await sendMonthlyEarningsDMs(prev);
      }

      currentMonth = nowMonth;

      const data = await buildLeaderboardsForMonth(nowMonth);
      const embed = new EmbedBuilder()
        .setTitle(`🏆 LEADERBOARD — ${nowMonth}`)
        .addFields(
          { name: "🔥 Top Inviters", value: data.inviteLines.join("\n") },
          { name: "💰 Top Affiliates", value: data.affiliateLines.join("\n") }
        );

      const msg = await findOrCreatePinnedLeaderboardMessage(lbChannel);
      await msg.edit({ content: null, embeds: [embed] });
    } catch (e) {
      console.error("LB tick error:", e);
    }
  }

  client.once(Events.ClientReady, async () => {
    console.log("✅ Leaderboards module ready.");
    await tick();
    setInterval(tick, 10 * 60 * 1000);
  });
}
