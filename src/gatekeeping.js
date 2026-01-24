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
    WAITLIST_INVITE_URL = "",
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

  // ✅ Local readiness flag: while restarting/warming up, don't leave interactions “thinking”
  let GK_READY = false;

  async function ensureGetAccessMessage() {
    const ch = await client.channels.fetch(String(GET_ACCESS_CHANNEL_ID)).catch(() => null);
    if (!ch || !ch.isTextBased()) return;

    const embed = new EmbedBuilder()
      .setTitle("🔒 Join the Waitlist")
      .setDescription(
        [
          "Access to this server is granted via a waitlist.",
          "",
          "Requests are reviewed regularly and prioritized for active accounts.",
          "",
          "**After joining the waitlist, you’ll receive an invite link by DM.**",
          "Inviting other is taken into account during approval.",
          "",
          "Click **Join Waitlist** below to request access.",
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

      if (member.roles.cache.has(String(MEMBER_ROLE_ID))) return;
      await member.roles.add(String(PENDING_ROLE_ID)).catch(() => {});
    } catch {}
  });

  // Button + modal handling
  client.on(Events.InteractionCreate, async (interaction) => {
    try {
      const cid = String(interaction.customId || "");

      // Only handle gatekeeping ids in this module
      const isGatekeeping = cid === GK.APPLY_BTN || cid === GK.APPLY_MODAL || cid.startsWith("gk_");
      if (!isGatekeeping) return;

      // ✅ If bot just restarted and is not ready yet, respond quickly (no endless "thinking")
      if (!GK_READY) {
        // For button interactions, we can reply quickly.
        if (interaction.isButton()) {
          await interaction
            .reply({
              content: "⚠️ Bot is restarting. Try again in ~10 seconds.",
              flags: MessageFlags.Ephemeral,
            })
            .catch(() => {});
        }
        // For modal submits, also reply quickly if possible
        if (interaction.isModalSubmit()) {
          await interaction
            .reply({
              content: "⚠️ Bot is restarting. Please submit again in ~10 seconds.",
              flags: MessageFlags.Ephemeral,
            })
            .catch(() => {});
        }
        return;
      }

      // Open modal
      if (interaction.isButton() && interaction.customId === GK.APPLY_BTN) {
        // IMPORTANT: do NOT defer; modals must be shown immediately
        await interaction.showModal(buildWaitlistModal()).catch(() => {});
        return;
      }

      // Modal submit -> Airtable record + DM
      if (interaction.isModalSubmit() && interaction.customId === GK.APPLY_MODAL) {
        const user = interaction.user;

        const country = interaction.fields.getTextInputValue(GK.COUNTRY)?.trim() || "";
        const ig = interaction.fields.getTextInputValue(GK.IG)?.trim() || "";
        const note = interaction.fields.getTextInputValue(GK.NOTE)?.trim() || "";

        // ✅ ACK immediately so the interaction never expires
        await interaction.deferReply({ flags: MessageFlags.Ephemeral }).catch(() => {});

        // Ensure Pending role if user is already in server (helps testing)
        try {
          const gid = String(ACCESS_GUILD_ID || interaction.guildId || "");
          if (gid) {
            const guild = await client.guilds.fetch(gid).catch(() => null);
            if (guild) {
              const member = await guild.members.fetch(user.id).catch(() => null);
              if (member) {
                if (
                  !member.roles.cache.has(String(MEMBER_ROLE_ID)) &&
                  !member.roles.cache.has(String(PENDING_ROLE_ID))
                ) {
                  await member.roles.add(String(PENDING_ROLE_ID)).catch(() => {});
                }
              }
            }
          }
        } catch {}

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

        // ✅ DM confirmation as EMBED + invite link
        const inviteUrl = String(WAITLIST_INVITE_URL || "").trim();

        const dmEmbed = new EmbedBuilder()
          .setThumbnail("https://i.imgur.com/FeMBxmk.png") // 👈 logo
          .setTitle("✅ You’re on the waitlist")
          .setDescription(
            [
              "Thank you for applying to join **Kickz Caviar Wholesale**", 
              "\n**Our mission** is to help buyers get better prices by buying together, opening access to trusted global suppliers.",
              "\n**More active, serious buyers** mean **better pricing** for everyone.",
              "\n🔗 **Know other serious buyers like you?** Use this invite link:",
              `\n${inviteUrl}`,
              "\nInvite activity is taken into account during approval.",
            ].join("\n")
          )
          .setColor(0xffd300);

        await user.send({ embeds: [dmEmbed] }).catch(() => {});

        await interaction
          .editReply("✅ You’re on the waitlist. Check your DMs for details + invite link.")
          .catch(() => {});
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
          filterByFormula: `AND({Status}='Approved', NOT({Discord Role Granted}))`,
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

    // Mark ready first (so interactions can respond quickly)
    GK_READY = true;

    // Delay non-critical work slightly so the bot is responsive immediately after deploy
    setTimeout(() => ensureGetAccessMessage().catch(() => {}), 1500);

    setInterval(pollAndGrant, POLL_MS);
    setTimeout(pollAndGrant, 10 * 1000);
  });
}
