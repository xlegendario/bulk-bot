import {
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  Events,
  MessageFlags,
} from "discord.js";

/**
 * Affiliate Invites Module
 *
 * Airtable tables expected:
 * - Discord Members:
 *    - Discord User ID (primary)
 *    - Discord Username
 *    - Invite Code
 *    - Invite URL
 *    - Invite Created At (date)
 *    - Joined At (date)
 *    - Invited By Discord User ID
 *    - Invite Code Used
 *
 * - Invites Log:
 *    - Invitee Discord User ID
 *    - Inviter Discord User ID
 *    - Invite Code Used
 *    - Joined At (date)
 *    - Month Key (formula)
 */
export function registerAffiliateInvites(ctx) {
  const { client, base, env } = ctx;

  const {
    AFFILIATE_CHANNEL_ID,
    AFFILIATE_GUILD_ID, // optional but recommended once stable
    AIRTABLE_MEMBERS_TABLE = "Discord Members",
    AIRTABLE_INVITES_LOG_TABLE = "Invites Log",
  } = env;

  if (!AFFILIATE_CHANNEL_ID) {
    console.warn("⚠️ Affiliate invites disabled: AFFILIATE_CHANNEL_ID missing.");
    return;
  }

  const membersTable = base(AIRTABLE_MEMBERS_TABLE);
  const invitesLogTable = base(AIRTABLE_INVITES_LOG_TABLE);

  const AI = {
    BTN_GET: "aff_get_invite",
  };

  // Map<guildId, Map<inviteCode, usesNumber>>
  const inviteCache = new Map();
  let AI_READY = false;

  // ---------- Helpers ----------
  function escapeAirtableValue(v) {
    return String(v || "").replace(/'/g, "\\'");
  }

  function guildAllowed(guildId) {
    if (!AFFILIATE_GUILD_ID) return true;
    return String(guildId) === String(AFFILIATE_GUILD_ID);
  }

  async function getAffiliateChannel(guild) {
    const ch = await guild.channels.fetch(String(AFFILIATE_CHANNEL_ID)).catch(() => null);
    if (!ch || !ch.isTextBased()) return null;
    return ch;
  }

  // ---------- Airtable helpers ----------
  async function findMemberRecordByDiscordId(discordId) {
    const rows = await membersTable
      .select({
        maxRecords: 1,
        filterByFormula: `{Discord User ID}='${escapeAirtableValue(discordId)}'`,
      })
      .firstPage()
      .catch((e) => {
        console.error("AI: Airtable select failed:", e);
        return [];
      });

    return rows?.[0] || null;
  }

  async function upsertMember(discordId, username, fields = {}) {
    const existing = await findMemberRecordByDiscordId(discordId);

    const payload = {
      "Discord User ID": String(discordId),
      "Discord Username": String(username || ""),
      ...fields,
    };

    if (existing) return await membersTable.update(existing.id, payload);
    return await membersTable.create(payload);
  }

  // ---------- Discord invite cache ----------
  async function refreshInviteCacheForGuild(guild) {
    try {
      const invites = await guild.invites.fetch(); // Collection<code, Invite>

      const snapshot = new Map();
      for (const [code, inv] of invites) {
        snapshot.set(code, Number(inv.uses || 0));
      }

      inviteCache.set(guild.id, snapshot);

      console.log(
        "AI: invite cache refreshed for guild",
        guild.id,
        "count:",
        snapshot.size
      );

      return true;
    } catch (e) {
      console.error("AI: invite fetch failed:", e);
      return false;
    }
  }

  // ---------- Affiliate message ----------
  async function ensureAffiliateMessage() {
    const ch = await client.channels.fetch(String(AFFILIATE_CHANNEL_ID)).catch(() => null);
    if (!ch || !ch.isTextBased()) return;

    const embed = new EmbedBuilder()
      .setTitle("🤝 Affiliate Program")
      .setDescription(
        [
          "Click below to get your **personal invite link**.",
          "",
          "• Monthly invite leaderboard",
          "• Earn **€5** per invited member that completes their **first deal**",
        ].join("\n")
      )
      .setColor(0xffd300);

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(AI.BTN_GET)
        .setLabel("Get my Invite URL")
        .setStyle(ButtonStyle.Primary)
    );

    const recent = await ch.messages.fetch({ limit: 25 }).catch(() => null);
    const existing = recent?.find(
      (m) => m.author?.id === client.user.id && m.embeds?.[0]?.title === "🤝 Affiliate Program"
    );

    if (existing) {
      await existing.edit({ embeds: [embed], components: [row] }).catch(() => {});
    } else {
      await ch.send({ embeds: [embed], components: [row] }).catch(() => {});
    }
  }

  // ---------- Create or reuse personal invite ----------
  async function getOrCreatePersonalInvite(guild, user) {
    const existing = await findMemberRecordByDiscordId(user.id);
    const existingUrl = existing?.fields?.["Invite URL"];
    if (existingUrl) {
      // keep cache fresh
      await refreshInviteCacheForGuild(guild);
      return { url: existingUrl, code: existing?.fields?.["Invite Code"] || "" };
    }

    const ch = await getAffiliateChannel(guild);
    if (!ch) throw new Error("Affiliate channel not found / not text-based.");

    const invite = await ch.createInvite({
      maxAge: 0,
      maxUses: 0,
      unique: true,
      reason: `Affiliate personal invite for ${user.tag} (${user.id})`,
    });

    await upsertMember(user.id, user.tag, {
      "Invite Code": invite.code,
      "Invite URL": invite.url,
      "Invite Created At": new Date().toISOString(),
    });

    // ✅ Critical: seed/refresh cache immediately so baseline contains the new code
    await refreshInviteCacheForGuild(guild);

    return { url: invite.url, code: invite.code };
  }

  // ---------- Ready ----------
  client.once(Events.ClientReady, async () => {
    try {
      await ensureAffiliateMessage();

      // Pre-cache invites (important for diff logic)
      if (AFFILIATE_GUILD_ID) {
        const g = await client.guilds.fetch(String(AFFILIATE_GUILD_ID)).catch(() => null);
        if (g) await refreshInviteCacheForGuild(g);
      } else {
        for (const [, g] of client.guilds.cache) {
          await refreshInviteCacheForGuild(g);
        }
      }

      // Optional periodic refresh (helps stability)
      setInterval(async () => {
        try {
          if (!AFFILIATE_GUILD_ID) return;
          const g = client.guilds.cache.get(String(AFFILIATE_GUILD_ID));
          if (g) await refreshInviteCacheForGuild(g);
        } catch (e) {
          console.error("AI: periodic cache refresh failed:", e);
        }
      }, 60_000);

      AI_READY = true;
      console.log("✅ Affiliate Invites module ready.");
    } catch (e) {
      console.error("AI: ready failed:", e);
      AI_READY = true;
    }
  });

  // Keep cache correct if invites are created/deleted
  client.on(Events.InviteCreate, async (invite) => {
    try {
      if (!invite?.guild?.id) return;
      if (!guildAllowed(invite.guild.id)) return;
      await refreshInviteCacheForGuild(invite.guild);
    } catch (e) {
      console.error("AI: InviteCreate handler error:", e);
    }
  });

  client.on(Events.InviteDelete, async (invite) => {
    try {
      if (!invite?.guild?.id) return;
      if (!guildAllowed(invite.guild.id)) return;
      const guild = invite.guild;
      if (guild) await refreshInviteCacheForGuild(guild);
    } catch (e) {
      console.error("AI: InviteDelete handler error:", e);
    }
  });

  // ---------- Button handler ----------
  client.on(Events.InteractionCreate, async (interaction) => {
    try {
      if (!interaction.isButton()) return;
      if (interaction.customId !== AI.BTN_GET) return;

      if (!AI_READY) {
        await interaction
          .reply({
            content: "⚠️ Bot is restarting. Try again in ~10 seconds.",
            flags: MessageFlags.Ephemeral,
          })
          .catch(() => {});
        return;
      }

      await interaction.deferReply({ flags: MessageFlags.Ephemeral }).catch(() => {});
      const guild = interaction.guild;

      if (!guild) {
        await interaction.editReply("⛔ This button only works in a server.").catch(() => {});
        return;
      }
      if (!guildAllowed(guild.id)) {
        await interaction.editReply("⛔ Wrong server.").catch(() => {});
        return;
      }

      const { url } = await getOrCreatePersonalInvite(guild, interaction.user);

      await interaction.editReply(
        [
          "✅ **Your personal invite link:**",
          url,
          "",
          "Copy-paste message:",
          `Join Kickz Caviar Wholesale: ${url}`,
        ].join("\n")
      );
    } catch (e) {
      console.error("AI: Interaction handler error:", e);
      try {
        if (interaction.deferred || interaction.replied) {
          await interaction.editReply("❌ Could not create your invite link. Ask staff.").catch(() => {});
        }
      } catch {}
    }
  });

  // ---------- Member join → detect used invite → log to Airtable ----------
  client.on(Events.GuildMemberAdd, async (member) => {
    try {
      const guild = member.guild;
      if (!guildAllowed(guild.id)) return;

      console.log("AI JOIN: fired", member.user.id, member.user.tag, "guild", guild.id);

      // Always log member join in Members table
      await upsertMember(member.user.id, member.user.tag, {
        "Joined At": new Date().toISOString(),
      }).catch((e) => console.error("AI JOIN: upsertMember(join) failed:", e));

      // 1) OLD snapshot (code -> uses)
      const oldUses = inviteCache.get(guild.id);
      console.log("AI JOIN: old snapshot?", !!oldUses, "size:", oldUses?.size ?? null);

      // 2) NEW fetch
      const newInvites = await guild.invites.fetch().catch((e) => {
        console.error("AI JOIN: guild.invites.fetch failed", e);
        return null;
      });
      console.log("AI JOIN: newInvites?", !!newInvites, "size:", newInvites?.size ?? null);

      if (!newInvites) return;

      // Build NEW snapshot (code -> uses)
      const newUses = new Map();
      for (const [code, inv] of newInvites) {
        newUses.set(code, Number(inv.uses || 0));
      }

      // Update cache immediately for next joins
      inviteCache.set(guild.id, newUses);

      if (!oldUses) {
        console.log("AI JOIN: no baseline snapshot yet -> cannot attribute this join (next will work).");
        return;
      }

      // 3) Find the invite code that increased the most
      let usedInvite = null;
      let bestDelta = 0;

      for (const [code, inv] of newInvites) {
        const oldU = Number(oldUses.get(code) || 0);
        const newU = Number(inv.uses || 0);
        const delta = newU - oldU;

        if (delta > bestDelta) {
          bestDelta = delta;
          usedInvite = inv;
        }
      }

      console.log(
        "AI JOIN: usedInvite",
        usedInvite?.code ?? null,
        "inviter",
        usedInvite?.inviter?.id ?? null
      );

      if (!usedInvite || !usedInvite.inviter?.id || bestDelta <= 0) {
        // Vanity URL / unknown join method / no increment detected
        return;
      }

      const inviterId = usedInvite.inviter.id;
      const inviteeId = member.user.id;

      // Ensure inviter exists
      await upsertMember(
        inviterId,
        usedInvite.inviter.tag || usedInvite.inviter.username || "",
        {}
      ).catch((e) => console.error("AI JOIN: upsertMember(inviter) failed:", e));

      // Update invitee inviter info ONLY if empty
      const inviteeRec = await findMemberRecordByDiscordId(inviteeId);
      const already = inviteeRec?.fields?.["Invited By Discord User ID"];
      if (inviteeRec && !already) {
        await membersTable
          .update(inviteeRec.id, {
            "Invited By Discord User ID": inviterId,
            "Invite Code Used": usedInvite.code,
          })
          .catch((e) => console.error("AI JOIN: update invitee inviter fields failed:", e));
      }

      // Leaderboard record
      await invitesLogTable
        .create({
          "Invitee Discord User ID": inviteeId,
          "Inviter Discord User ID": inviterId,
          "Invite Code Used": usedInvite.code,
          "Joined At": new Date().toISOString(),
        })
        .catch((e) => console.error("AI JOIN: Invites Log create failed:", e));

      console.log("AI JOIN: ✅ logged invite -> Inviter:", inviterId, "Invitee:", inviteeId);
    } catch (e) {
      console.error("AI: GuildMemberAdd error:", e);
    }
  });
}
