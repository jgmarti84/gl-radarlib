#!/usr/bin/env bash
# Single source of truth = the project skill/commands under .claude/.
# This regenerates the distributable plugin bundle (.claude/plugins/radarlib-ops)
# from them. Run it before packaging/distributing the plugin.
#
#   bash .claude/skills/radar-doctor/sync_plugin.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

SKILL_SRC=".claude/skills/radar-doctor"
PLUGIN=".claude/plugins/radarlib-ops"

mkdir -p "$PLUGIN/skills" "$PLUGIN/commands" "$PLUGIN/agents"
rm -rf "$PLUGIN/skills/radar-doctor"
cp -r "$SKILL_SRC" "$PLUGIN/skills/radar-doctor"
rm -f "$PLUGIN/skills/radar-doctor/sync_plugin.sh"   # don't ship the generator

# Ship every radar-* command and the fleet agent.
for f in .claude/commands/radar-*.md; do
  cp "$f" "$PLUGIN/commands/$(basename "$f")"
done
for f in .claude/agents/radar-*.md; do
  [ -e "$f" ] && cp "$f" "$PLUGIN/agents/$(basename "$f")"
done

echo "Synced project skill/commands -> $PLUGIN"
find "$PLUGIN" -type f | sort
