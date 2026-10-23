#!/usr/bin/env python3
"""Render docs/DATA_DICTIONARY.md as a production PDF."""

from __future__ import annotations

from pathlib import Path

from fpdf import FPDF

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "DATA_DICTIONARY.md"
OUTPUT = ROOT / "docs" / "DATA_DICTIONARY.pdf"
FONT = Path(r"C:\Windows\Fonts\arial.ttf")
FONT_BOLD = Path(r"C:\Windows\Fonts\arialbd.ttf")


def clean(text: str) -> str:
    return (
        text.replace("`", "")
        .replace("──<", "-<")
        .replace(">──", ">-")
        .replace("├──<", "+-<")
        .replace("└──<", "+-<")
        .replace("├──", "+-")
        .replace("└──", "+-")
        .replace("│", "|")
    )


def blocks(markdown: str) -> list[tuple[str, object]]:
    items: list[tuple[str, object]] = []
    lines = markdown.replace("\r\n", "\n").split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if line.startswith("# "):
            items.append(("h1", clean(line[2:].strip())))
            i += 1
            continue
        if line.startswith("## "):
            items.append(("h2", clean(line[3:].strip())))
            i += 1
            continue
        if line.startswith("```"):
            chunk = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                chunk.append(clean(lines[i]))
                i += 1
            i += 1
            items.append(("code", "\n".join(chunk)))
            continue
        if line.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                cells = [clean(cell.strip()) for cell in lines[i].strip().strip("|").split("|")]
                if not all(set(cell) <= set("-: ") for cell in cells):
                    rows.append(cells)
                i += 1
            items.append(("table", rows))
            continue
        if line.startswith("- "):
            bullets = []
            while i < len(lines) and lines[i].startswith("- "):
                bullets.append(clean(lines[i][2:].strip()))
                i += 1
            items.append(("list", bullets))
            continue
        paragraph = [clean(line.strip())]
        i += 1
        while i < len(lines) and lines[i].strip() and not lines[i].startswith(("#", "|", "-", "`")):
            paragraph.append(clean(lines[i].strip()))
            i += 1
        items.append(("p", " ".join(paragraph)))
    return items


class DictionaryPDF(FPDF):
    def header(self) -> None:
        if self.page_no() == 1:
            return
        self.set_font("Body", "", 8)
        self.set_text_color(90, 90, 90)
        self.cell(0, 8, "cricsocc.bronze production data dictionary", align="L")
        self.ln(10)

    def footer(self) -> None:
        self.set_y(-12)
        self.set_font("Body", "", 8)
        self.set_text_color(90, 90, 90)
        self.cell(0, 8, f"Page {self.page_no()}/{{nb}}  ·  Snapshot 23 October 2026", align="C")


def render() -> None:
    pdf = DictionaryPDF(format="A4")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=16)
    pdf.add_font("Body", "", str(FONT))
    pdf.add_font("Body", "B", str(FONT_BOLD))
    pdf.add_page()
    pdf.set_left_margin(14)
    pdf.set_right_margin(14)

    for kind, value in blocks(SOURCE.read_text(encoding="utf-8")):
        if kind == "h1":
            pdf.set_font("Body", "B", 20)
            pdf.set_text_color(20, 20, 20)
            pdf.multi_cell(0, 10, value)
            pdf.ln(1)
            pdf.set_font("Body", "", 10)
            pdf.set_text_color(80, 80, 80)
            pdf.multi_cell(0, 5, "Catalog cricsocc  ·  Schema bronze  ·  Managed Delta")
            pdf.ln(4)
            continue
        if kind == "h2":
            if pdf.get_y() > 250:
                pdf.add_page()
            pdf.ln(2)
            pdf.set_font("Body", "B", 13)
            pdf.set_text_color(20, 20, 20)
            pdf.multi_cell(0, 7, value)
            pdf.ln(1)
            continue
        if kind == "p":
            pdf.set_x(pdf.l_margin)
            pdf.set_font("Body", "", 10)
            pdf.set_text_color(30, 30, 30)
            pdf.multi_cell(0, 5, value)
            pdf.ln(2)
            continue
        if kind == "list":
            pdf.set_font("Body", "", 10)
            pdf.set_text_color(30, 30, 30)
            for item in value:
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(0, 5, f"-  {item}")
            pdf.ln(2)
            continue
        if kind == "code":
            pdf.set_fill_color(245, 245, 245)
            pdf.set_font("Body", "", 9)
            pdf.set_text_color(30, 30, 30)
            pdf.multi_cell(0, 4.5, value, fill=True)
            pdf.ln(3)
            continue
        if kind == "table":
            rows = value
            if not rows:
                continue
            width = pdf.epw
            count = len(rows[0])
            if count == 4 and rows[0][0] == "Table":
                widths = (width * 0.20, width * 0.38, width * 0.14, width * 0.28)
            elif count == 4:
                widths = (width * 0.22, width * 0.18, width * 0.10, width * 0.50)
            elif count == 2:
                widths = (width * 0.38, width * 0.62)
            else:
                widths = tuple(width / count for _ in range(count))
            pdf.set_font("Body", "", 8)
            with pdf.table(
                col_widths=widths,
                text_align=("LEFT",) * count,
                line_height=4.4,
                markdown=False,
            ) as table:
                for index, row in enumerate(rows):
                    cells = table.row()
                    for cell in row:
                        if index == 0:
                            pdf.set_font("Body", "B", 8)
                        else:
                            pdf.set_font("Body", "", 8)
                        cells.cell(cell)
            pdf.ln(3)
            pdf.set_x(pdf.l_margin)

    pdf.output(OUTPUT)
    print(OUTPUT)


if __name__ == "__main__":
    render()
