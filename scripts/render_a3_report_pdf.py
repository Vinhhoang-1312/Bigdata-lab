#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from pypdf import PdfReader

root = Path(__file__).resolve().parents[1]
md = (root / 'reports' / 'a3_final_report.md').read_text(encoding='utf-8')
out = root / 'reports' / 'a3_final_report.pdf'
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='SmallCode', parent=styles['Code'], fontSize=8, leading=10))
styles.add(ParagraphStyle(name='Tiny', parent=styles['BodyText'], fontSize=8, leading=10))
styles['BodyText'].fontSize = 10
styles['BodyText'].leading = 13

def clean(s):
    return s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

story=[]
in_code=False
section_count=0
for line in md.splitlines():
    if line.startswith('```'):
        in_code = not in_code
        continue
    if in_code:
        story.append(Paragraph(clean(line), styles['SmallCode'])); story.append(Spacer(1,0.08*cm)); continue
    if not line.strip():
        story.append(Spacer(1, 0.18*cm)); continue
    if line.startswith('# '):
        story.append(Paragraph(clean(line[2:]), styles['Title'])); story.append(Spacer(1,0.35*cm)); continue
    if line.startswith('## '):
        section_count += 1
        if section_count in {3,5,7}:
            story.append(PageBreak())
        story.append(Paragraph(clean(line[3:]), styles['Heading2'])); story.append(Spacer(1,0.12*cm)); continue
    if line.startswith('|'):
        cells=[clean(x.strip(' `')) for x in line.strip('|').split('|')]
        if set(cells[0]) <= {'-', ':'}:
            continue
        story.append(Paragraph(clean(' | '.join(cells)), styles['Tiny']))
        story.append(Spacer(1,0.06*cm))
        continue
    if line.startswith('- '):
        story.append(Paragraph('&bull; ' + clean(line[2:]), styles['BodyText'])); story.append(Spacer(1,0.10*cm)); continue
    story.append(Paragraph(clean(line), styles['BodyText'])); story.append(Spacer(1,0.10*cm))

# Add compact appendix pages from audit to meet 5-10 page PDF deliverable with requirement traceability.
story.append(PageBreak())
story.append(Paragraph('Appendix: requirement checkpoint audit', styles['Heading2']))
audit = (root / 'reports' / 'a3_requirement_audit.md').read_text(encoding='utf-8')
for line in audit.splitlines():
    if line.startswith('|') and not set(line.strip('|').split('|')[0].strip()) <= {'-', ':'}:
        story.append(Paragraph(clean(line.strip('|').replace('|', ' | ')), styles['Tiny']))
    elif line.startswith('- '):
        story.append(Paragraph('&bull; ' + clean(line[2:]), styles['Tiny']))
    elif line.startswith('#'):
        continue
    elif line.strip():
        story.append(Paragraph(clean(line), styles['Tiny']))
    story.append(Spacer(1,0.04*cm))

doc=SimpleDocTemplate(str(out), pagesize=A4, rightMargin=1.6*cm, leftMargin=1.6*cm, topMargin=1.3*cm, bottomMargin=1.3*cm)
doc.build(story)
reader=PdfReader(str(out))
text='\n'.join(page.extract_text() or '' for page in reader.pages)
print(out)
print('pages', len(reader.pages))
print('has_step5', 'Step 5' in text)
print('has_micro_f1', 'micro-F1' in text)
assert 5 <= len(reader.pages) <= 10
assert 'Step 5' in text and 'micro-F1' in text
