# this is for linux/wsl
# run these commands yourself to get started

# install homebrew - this helps install other items
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# install uv - used to manage python venv and lib installs
brew install uv

# get uv to install python and libs
uv sync
