import os
import sys

sys.path.insert(0, os.path.abspath("../../src"))

project = "SQL Data Guard"
copyright = "2026, Imperva - Threat Research Infra"
author = "Imperva - Threat Research Infra"
release = "0.0.1"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = []

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
