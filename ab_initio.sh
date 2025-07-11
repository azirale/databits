# if you are on windows use wsl2, it makes all data work significantly easier
# run these commands yourself to get started

# install homebrew (https://brew.sh/) - this helps install other items
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# install uv - used to manage python venv and lib installs
brew install uv

# install ruff - very nice for python formatting
brew install ruff

# get uv to install python and libs based on pyproject.toml
uv sync
