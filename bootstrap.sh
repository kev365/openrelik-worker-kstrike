#!/bin/bash
# This small script will bootstrap the new worker code.
echo "Please answer the below question to bootstrap your initial worker code."
echo
echo -n "Workername? "
read workername
echo -n "Your name? "
read yourname
echo -n "Your email address? "
read youremail
echo -n "A short one line description of the worker? "
read onelinedescription

# Portable sed in-place: detect GNU vs BSD sed
if sed --version 2>&1 | grep -q GNU; then
  SED_INPLACE="sed -i"
else
  SED_INPLACE="sed -i ''"
fi

# Replace template placeholders
$SED_INPLACE "s/TEMPLATEWORKERNAME/$workername/g" pyproject.toml
$SED_INPLACE "s/TEMPLATENAME/$yourname/g" pyproject.toml
$SED_INPLACE "s/TEMPLATEEMAIL/$youremail/g" pyproject.toml
$SED_INPLACE "s/TEMPLATEWORKERNAME/$workername/g" src/tasks.py
$SED_INPLACE "s/TEMPLATEDESC/$onelinedescription/g" src/tasks.py
$SED_INPLACE "s/TEMPLATEWORKERNAME/$workername/g" README.md
$SED_INPLACE "s/TEMPLATEWORKERNAME/$workername/g" openrelik.yaml
$SED_INPLACE "s/TEMPLATEWORKERNAME/$workername/g" Dockerfile

echo
echo "Bootstrap complete! Your worker '$workername' is ready."
echo "Next steps:"
echo "  1. Write tests in tests/"
echo "  2. Implement worker logic in src/tasks.py"
echo "  3. Run tests: uv sync --group test && uv run pytest -s --cov=."
