#!/usr/bin/env python3
"""Regenerate the Assignment 3 Markdown source from verified evidence."""
from render_a3_report_pdf import MD_OUT, load_data, write_markdown

if __name__ == "__main__":
    write_markdown(load_data())
    print(MD_OUT)
