// backfillDiscordMembers.js
export async function backfillDiscordMembers(ctx) {
  const { client, base, env } = ctx;

  const {
    AFFILIATE_GUILD_ID,
    AIRTABLE_MEMBERS_TABLE = "Discord Members",
  } = env;

  const membersTable = base(AIRTABLE_MEMBERS_TABLE);

  const guild = await client.guilds.fetch(String(AFFILIATE_GUILD_ID));
  const members = await guild.members.fetch();

  console.log(`🧹 Backfill started — ${members.size} members found`);

  for (const [, m] of members) {
    const discordId = String(m.user.id);

    const existing = await membersTable
      .select({
        maxRecords: 1,
        filterByFormula: `{Discord User ID}='${discordId}'`,
      })
      .firstPage();

    if (existing.length) continue;

    await membersTable.create({
      "Discord User ID": discordId,
      "Discord Username": m.user.tag,
      "Joined At": m.joinedAt ? m.joinedAt.toISOString() : null,
    });

    console.log("➕ Backfilled:", m.user.tag);
  }

  console.log("✅ Backfill complete");
}
