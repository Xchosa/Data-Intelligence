#!/bin/bash

# Close System Preferences / System Settings to prevent sync issues
osascript -e 'tell application "System Settings" to quit' 2>/dev/null
osascript -e 'tell application "System Preferences" to quit' 2>/dev/null

echo "Starting Mac-to-Windows/Linux transformation..."

###############################################################################
# 1. KEYBOARD REPEAT RATES                                                    #
###############################################################################
# Reset to Mac standard defaults
defaults write NSGlobalDomain KeyRepeat -int 2
defaults write NSGlobalDomain InitialKeyRepeat -int 15
defaults write -g ApplePressAndHoldEnabled -bool true

###############################################################################
# 2. SWAP CTRL AND CMD                                                        #
###############################################################################
# This makes:
# - Physical Ctrl behave like Command
# - Physical Command behave like Control
#
# Result:
# Ctrl+C = Copy
# Ctrl+V = Paste
# Ctrl+A = Select All
# Ctrl+Z = Undo
#
# Note: This mapping is session-based and may reset after reboot/login.
hidutil property --set '{
  "UserKeyMapping": [
    {
      "HIDKeyboardModifierMappingSrc": 0x7000000E0,
      "HIDKeyboardModifierMappingDst": 0x7000000E3
    },
    {
      "HIDKeyboardModifierMappingSrc": 0x7000000E3,
      "HIDKeyboardModifierMappingDst": 0x7000000E0
    }
  ]
}'


###############################################################################
# 3. CHROME SHORTCUT ADJUSTMENTS                                              #
###############################################################################
# Since physical Ctrl is now interpreted as Command, most standard shortcuts
# already behave as desired. This block keeps Ctrl+Tab / Ctrl+Shift+Tab for
# Chrome tab navigation.
defaults write com.google.Chrome NSUserKeyEquivalents -dict-add "Select Next Tab" "^\\t"
defaults write com.google.Chrome NSUserKeyEquivalents -dict-add "Select Previous Tab" "^$\\t"

###############################################################################
# 4. MOUSE & UI SETTINGS                                                      #
###############################################################################
# Disable Natural Scrolling
defaults write -g com.apple.swipescrolldirection -bool false

# Dark Mode
osascript -e 'tell app "System Events" to tell appearance preferences to set dark mode to true'

# Finder: Show hidden files and all file extensions
defaults write com.apple.finder AppleShowAllFiles -bool true
defaults write NSGlobalDomain AppleShowAllExtensions -bool true
defaults write com.apple.finder _FXSortFoldersFirst -bool true

# Dock: small, hidden, instant animation
defaults write com.apple.dock tilesize -int 20
defaults write com.apple.dock autohide -bool true
defaults write com.apple.dock autohide-delay -float 0
defaults write com.apple.dock autohide-time-modifier -float 0
defaults write com.apple.dock show-recents -bool false

###############################################################################
# 5. APPLY CHANGES                                                            #
###############################################################################
echo "Refreshing system services..."

for app in "Dock" "Finder" "SystemUIServer" "Google Chrome"; do
  killall "$app" &>/dev/null
done

echo "-----------------------------------------------------------"
echo "Setup complete."
echo "Physical Ctrl now behaves like Command:"
echo "  Ctrl+C = Copy"
echo "  Ctrl+V = Paste"
echo "  Ctrl+A = Select All"
echo "  Ctrl+Z = Undo"
echo ""
echo "Physical Command now behaves like Control."
echo ""
echo "Note:"
echo "- You may need to log out and back in for repeat rates."
echo "- hidutil key remapping may reset after reboot/login."
echo "-----------------------------------------------------------"