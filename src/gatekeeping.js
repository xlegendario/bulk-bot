import {
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  Events,
  MessageFlags,
} from "discord.js";

export function registerGatekeeping(ctx) {
  const { client, base, env } = ctx;

  const {
    ACCESS_WAITLIST_TABLE = "Access Waitlist",
    GET_ACCESS_CHANNEL_ID,
    ACCESS_GUILD_ID,
    PENDING_ROLE_ID,
    MEMBER_ROLE_ID,
    WAITLIST_INVITE_URL = "", // optional, for DM after apply
  } = env;

  if (!GET_ACCESS_CHANNEL_ID || !PENDING_ROLE_ID || !MEMBER_ROLE_ID) {
    console.warn(
      "⚠️ Gatekeeping disabled: missing env vars (GET_ACCESS_CHANNEL_ID / PENDING_ROLE_ID / MEMBER_ROLE_ID)."
    );
    return;
  }

  const waitlistTable = base(ACCESS_WAITLIST_TABLE);

  const GK = {
    APPLY_BTN: "gk_waitlist_apply",
    APPLY_MODAL: "gk_waitlist_modal",
    COUNTRY: "gk_country",
    IG: "gk_ig",
    NOTE: "gk_note",
  };

  async function ensureGetAccessMessage() {
    const ch = await client.channels.fetch(String(GET_ACCESS_CHANNEL_ID)).catch(() => null);
    if (!ch || !ch.isTextBased()) return;

    const embed = new EmbedBuilder()
      .setTitle("🔒 Join the Waitlist")
      .setDescription(
        [
          "Access to this server is granted via a waitlist.",
          "",
          "Click **Join Waitlist** below to request access.",
          "Requests are reviewed regularly.",
          "",
          "*Invite activity may be taken into account when granting access.*",
        ].join("\n")
      )
      .setColor(0xffd300);

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(GK.APPLY_BTN)
        .setLabel("Join Waitlist")
        .setEmoji("📝")
        .setStyle(ButtonStyle.Primary)
    );

    const recent = await ch.messages.fetch({ limit: 25 }).catch(() => null);
    const existing = recent?.find(
      (m) => m.author?.id === client.user.id && m.embeds?.[0]?.title === "🔒 Join the Waitlist"
    );

    if (existing) await existing.edit({ embeds: [embed], components: [row] }).catch(() => {});
    else await ch.send({ embeds: [embed], components: [row] }).catch(() => {});
  }

  function buildWaitlistModal() {
    const modal = new ModalBuilder().setCustomId(GK.APPLY_MODAL).setTitle("Join Waitlist");

    const country = new TextInputBuilder()
      .setCustomId(GK.COUNTRY)
      .setLabel("Country")
      .setStyle(TextInputStyle.Short)
      .setRequired(true);

    const ig = new TextInputBuilder()
      .setCustomId(GK.IG)
      .setLabel("Instagram / Website (optional)")
      .setStyle(TextInputStyle.Short)
      .setRequired(false);

    const note = new TextInputBuilder()
      .setCustomId(GK.NOTE)
      .setLabel("Anything we should know? (optional)")
      .setStyle(TextInputStyle.Paragraph)
      .setRequired(false);

    modal.addComponents(
      new ActionRowBuilder().addComponents(country),
      new ActionRowBuilder().addComponents(ig),
      new ActionRowBuilder().addComponents(note)
    );

    return modal;
  }

  // Give Pending role on join
  client.on(Events.GuildMemberAdd, async (member) => {
    try {
      const expectedGuildId = String(ACCESS_GUILD_ID || member.guild.id);
      if (String(member.guild.id) !== expectedGuildId) return;

      // If they already have Member (rejoin edge case), don't force Pending
      if (member.roles.cache.has(String(MEMBER_ROLE_ID))) return;

      await member.roles.add(String(PENDING_ROLE_ID)).catch(() => {});
    } catch {}
  });

  // Button + modal handling
  client.on(Events.InteractionCreate, async (interaction) => {
    try {
      // Open modal
      if (interaction.isButton() && interaction.customId === GK.APPLY_BTN) {
        // IMPORTANT: do NOT defer; modals must be shown immediately
        await interaction.showModal(buildWaitlistModal()).catch(() => {});
        return;
      }

      // Modal submit -> Airtable record
      if (interaction.isModalSubmit() && interaction.customId === GK.APPLY_MODAL) {
        const user = interaction.user;

        const country = interaction.fields.getTextInputValue(GK.COUNTRY)?.trim() || "";
        const ig = interaction.fields.getTextInputValue(GK.IG)?.trim() || "";
        const note = interaction.fields.getTextInputValue(GK.NOTE)?.trim() || "";

        // ✅ ACK immediately so the interaction never expires (10062)
        await interaction.deferReply({ flags: MessageFlags.Ephemeral }).catch(() => {});

        // If user is already in server and doesn't have Pending, add it (makes testing easier)
        try {
          const gid = String(ACCESS_GUILD_ID || interaction.guildId || "");
          if (gid) {
            const guild = await client.guilds.fetch(gid).catch(() => null);
            if (guild) {
              const member = await guild.members.fetch(user.id).catch(() => null);
              if (member) {
                if (!member.roles.cache.has(String(MEMBER_ROLE_ID)) && !member.roles.cache.has(String(PENDING_ROLE_ID))) {
                  await member.roles.add(String(PENDING_ROLE_ID)).catch(() => {});
                }
              }
            }
          }
        } catch {}

        console.log("GK: submit received from", user.id, user.tag, "country:", country);

        // Prevent duplicates: update existing record if user already applied
        const existing = await waitlistTable
          .select({
            maxRecords: 1,
            filterByFormula: `{Discord User ID}='${String(user.id).replace(/'/g, "\\'")}'`,
          })
          .firstPage()
          .catch((e) => {
            console.error("GK: Airtable select failed:", e);
            return [];
          });

        const fields = {
          "Discord User ID": user.id,
          "Discord Username": user.tag,
          "Country": country,
          "Instagram / Website": ig,
          "Note": note,
          "Status": "Pending",
          "Applied At": new Date().toISOString(),

          // Use proper types for Airtable:
          "Discord Role Granted": false,
          "Granted At": null,
        };

        try {
          if (existing.length) {
            await waitlistTable.update(existing[0].id, fields);
            console.log("GK: updated waitlist record for", user.id);
          } else {
            const created = await waitlistTable.create(fields);
            console.log("GK: created waitlist record", created.id, "for", user.id);
          }
        } catch (e) {
          console.error("GK: Airtable write failed:", e);
          await interaction.editReply("⚠️ Could not save your request. Please try again in 1 minute.").catch(() => {});
          return;
        }

        // ✅ DM the user a confirmation + invite link (optional)
        const inviteUrl = String(env.WAITLIST_INVITE_URL || "").trim();

        const dmText = [
          "✅ **Thanks — you’re on the waitlist.**",
          "",
          "Kickz Caviar Wholesale exists to give more buyers access to **group bulk buying** and a **supplier network** they normally wouldn’t reach alone.",
          "",
          "We review requests regularly — you’ll get access as soon as you’re approved.",
          "",
          inviteUrl ? `🔗 **Invite link you can share:** ${inviteUrl}` : null,
          "Invite activity may be taken into account when granting access.",
        ].filter(Boolean).join("\n");

        await interaction.user.send(dmText).catch(() => {});
        }

        await interaction.editReply("✅ You’re on the waitlist. We’ll review requests regularly.").catch(() => {});
        return;
      }
    } catch (e) {
      console.error("Gatekeeping interaction error:", e);
    }
  });

  // Poll Airtable for Approved records and grant roles
  const POLL_MS = 60 * 1000;
  const MAX_GRANTS_PER_TICK = 25;

  async function pollAndGrant() {
    try {
      const rows = await waitlistTable
        .select({
          maxRecords: 200,
          filterByFormula: `AND(
            {Status}='Approved',
            NOT({Discord Role Granted})
          )`,
        })
        .firstPage();

      if (!rows.length) return;

      const guildId = String(ACCESS_GUILD_ID);
      const guild = guildId ? await client.guilds.fetch(guildId).catch(() => null) : null;
      if (!guild) return;

      let processed = 0;

      for (const r of rows) {
        if (processed >= MAX_GRANTS_PER_TICK) break;

        const discordId = String(r.fields?.["Discord User ID"] || "").trim();
        if (!discordId) continue;

        const member = await guild.members.fetch(discordId).catch(() => null);
        if (!member) continue;

        await member.roles.add(String(MEMBER_ROLE_ID)).catch(() => {});
        await member.roles.remove(String(PENDING_ROLE_ID)).catch(() => {});

        await waitlistTable
          .update(r.id, {
            "Discord Role Granted": true,
            "Granted At": new Date().toISOString(),
          })
          .catch((e) => console.error("GK: failed to update granted flags:", e));

        processed++;
      }

      if (processed > 0) console.log(`🔐 Gatekeeping: granted access to ${processed} user(s).`);
    } catch (e) {
      console.warn("⚠️ Gatekeeping poll failed:", e?.message || e);
    }
  }

  client.once(Events.ClientReady, async () => {
    console.log("✅ Gatekeeping ready. Table:", ACCESS_WAITLIST_TABLE);
    await ensureGetAccessMessage();
    setInterval(pollAndGrant, POLL_MS);
    setTimeout(pollAndGrant, 10 * 1000);
  });
}
