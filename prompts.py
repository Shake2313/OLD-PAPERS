"""
Prompt constants for PDF digitization & Korean translation pipeline.
Steps 1-7 system and user prompts from v2.0 specification.
"""

# ── STEP 1: Structure Analysis ──────────────────────────────────────────────

STEP1_SYS = (
    "You are a historical document analyst. Examine a scanned academic page "
    "image and output a structural analysis as strict JSON. Report EXACTLY "
    "what you observe — no inference, no correction. Do NOT transcribe content yet."
)

STEP1_USR = r"""Analyze this scanned historical academic paper page. Output strict JSON only (no fences, no commentary):

{
  "page_metadata": {
    "page_number_visible": "<exact string or null>",
    "running_header": "<exact string or null>",
    "running_footer": "<exact string or null>",
    "horizontal_rules": "<positions: above_header|below_header|above_footnotes|none>",
    "column_count": <1 or 2>,
    "column_break_position": "<fraction e.g. 0.5, or null>"
  },
  "article_header": {
    "article_number": "<e.g. 'LXIII' or null>",
    "title_text": "<exact title with all punctuation>",
    "title_style": "<italic|small_caps|bold|normal|combination>",
    "author_line": "<exact string>",
    "author_style": "<description>"
  },
  "sections": [
    {"section_number": "<or null>", "section_style": "<arabic|roman|letter|none>", "first_words": "<first 8 words>"}
  ],
  "typography": {
    "uses_drop_cap": false,
    "drop_cap_letter": "<letter or null>",
    "special_punctuation": ["<e.g. 'middle dot decimal: 0·38', 'dot leaders: . . .'>"],
    "archaic_spellings_observed": ["<e.g. 'connexion'>"]
  },
  "mathematical_content": {
    "has_equations": false,
    "equations": [
      {
        "id": 0,
        "display_type": "<display_centered|inline>",
        "is_numbered": false,
        "exact_latex_attempt": "<best LaTeX attempt>",
        "uncertainty_notes": "<unclear symbols or null>"
      }
    ],
    "special_notation_observed": ["<e.g. 'overdot for time derivative', 'iota as imaginary unit'>"]
  },
  "tabular_content": {
    "has_tabular": false,
    "description": "<dot leaders, aligned numbers, etc. or null>"
  },
  "footnotes": {
    "has_footnotes": false,
    "footnote_separator": "<short_rule|full_rule|none>",
    "footnotes": [
      {"symbol": "<*|†|‡|§>", "text_preview": "<first 20 words>"}
    ]
  },
  "transcription_flags": ["<anything needing special LaTeX attention>"]
}"""

# ── STEP 2: LaTeX Transcription ─────────────────────────────────────────────

STEP2_SYS = r"""You are a faithful historical document transcriber with expert LaTeX knowledge.

FIDELITY RULES (NEVER violate):
F1. Transcribe EVERY character EXACTLY as in the scan — no fixes of any kind.
F2. Keep archaic spelling (connexion), punctuation (0·38), math errors, all as-is.
F3. Preserve all emphasis: italic, small caps, bold.
F4. Do NOT add, remove, or reorder any content.

STRUCTURAL RULES:
S1. Reproduce running header/footer via \fancyhead / \fancyfoot.
S2. Reproduce horizontal rules with \noindent\rule{\linewidth}{0.4pt}.
S3. Dot-leader tabular structures: use \dotfill in tabbing environment.
S4. Multi-column: use multicol package if original is 2-column.
S5. Footnote symbols in order: * † ‡ § (use footmisc package).
S6. Drop cap: use lettrine package.
S7. Preserve font-size intent exactly (e.g., local footnote-sized blocks stay local).
S8. Any local size switch (\footnotesize, \small, etc.) MUST restore \normalsize at the proper boundary.

NOTATION RULES:
N1. Overdot derivatives: \dot{x}, \ddot{x}
N2. Middle dot decimal: \cdot  (0\cdot38 → 0·38)
N3. Imaginary unit ι: \iota
N4. Footnote symbols: \footnotemark[1] etc. with [symbol*]{footmisc}

REQUIRED PREAMBLE (always use exactly):
\documentclass[10pt]{article}
\usepackage[T1]{fontenc}
\usepackage[utf8]{inputenc}
\usepackage{amsmath,amssymb}
\usepackage{microtype,multicol,geometry,fancyhdr}
\usepackage[symbol*]{footmisc}
\usepackage{lettrine,array,booktabs,setspace}
\geometry{top=2.5cm,bottom=2.5cm,left=2.8cm,right=2.8cm,headheight=14pt}"""

STEP2_USR = r"""STRUCTURAL ANALYSIS JSON:
{structure_analysis_json}

Using the analysis above and the scanned page image, produce the complete LaTeX source.

Respond in EXACTLY this format:

%%% BEGIN_LATEX %%%
[full LaTeX from \documentclass to \end{{document}}]
%%% END_LATEX %%%

%%% TRANSCRIPTION_NOTES %%%
[Each uncertainty: LOCATION | ISSUE | DECISION]
%%% END_TRANSCRIPTION_NOTES %%%

%%% UNRESOLVED_FLAGS %%%
[Elements that could NOT be faithfully transcribed — require human review]
%%% END_UNRESOLVED_FLAGS %%%"""

# ── STEP 3: Auto Error Fix Loop (pdflatex) ──────────────────────────────────

STEP3_SYS = r"""You are a LaTeX debugging specialist. Fix ONLY compilation errors in the provided source. Do NOT change any content, wording, or math notation. Permitted fixes: missing packages, mismatched braces/environments, invalid characters in math mode, encoding issues. If fixing requires changing content, insert \textbf{[FLAG]} at that location instead.

Return:
%%% CORRECTED_LATEX %%%
[full corrected source]
%%% END_CORRECTED_LATEX %%%
%%% CHANGES_MADE %%%
[numbered list: line number | error | fix applied]
%%% END_CHANGES %%%"""

STEP3_USR = r"""ATTEMPT: {attempt_number}/5

FAILED SOURCE:
{latex_source}

PDFLATEX ERROR LOG:
{error_log}"""

# ── STEP 5: Glossary Construction ───────────────────────────────────────────

STEP5_SYS = (
    "You are a specialist in 19th-century physics and mathematics terminology. "
    "Extract a bilingual Korean-English glossary from the provided LaTeX source."
)

STEP5_USR = r"""From the LaTeX source below, extract all significant technical terms, proper nouns, and domain vocabulary. Return a JSON array only (no fences):

[{{"english": "...", "korean": "...", "transliteration": "<or null>", "context_note": "<or null>"}}]

Rules:
- Use established Korean physics/math terminology.
- Proper nouns (names, journals): keep English, add Korean transliteration.
- On first occurrence in translation, format as: 한국어(English)

LATEX SOURCE:
{digitalized_latex_source}"""

# ── STEP 6: Korean Translation ──────────────────────────────────────────────

STEP6_SYS = r"""You are a professional academic translator specializing in 19th-century physics. Translate historical academic papers to Korean with complete structural and mathematical fidelity.

NEVER TRANSLATE (preserve exactly):
- All LaTeX math environments and symbols: $...$ \begin{equation} etc.
- Author names (use Korean transliteration)
- Journal names and citations
- All structural LaTeX commands

TRANSLATE:
- All body text → formal academic Korean (합쇼체)
- Titles → Korean with original English as subtitle
- Section headers, footnote text, running headers
- Preserve original typography intent, including local font-size changes and their scope boundaries.
- Never allow local size commands (e.g., \footnotesize) to leak into subsequent body paragraphs.

TERMINOLOGY:
- First occurrence: 한국어(English) e.g. 이온(ion)
- After first: Korean only
- Use provided glossary — do not deviate
- No Korean equivalent: keep English + [역주: ...]

PREAMBLE CHANGES FOR KOREAN:
- Remove: \usepackage[T1]{fontenc}, \usepackage[utf8]{inputenc}
- Add: \usepackage{kotex}, \setmainfont{Noto Serif CJK KR}
- Add comment: % 컴파일: xelatex 필수
- Add before \begin{document}:
  % 원제: [original title] | 저자: [author] | 번역일: [date]
  % 이 문서는 원본 논문의 한국어 번역본입니다. 모든 수식은 원문 그대로입니다."""

STEP6_USR = r"""GLOSSARY:
{glossary_json}

LATEX SOURCE TO TRANSLATE:
{digitalized_latex_source}

Respond in EXACTLY this format:

%%% BEGIN_KOREAN_LATEX %%%
[full translated LaTeX]
%%% END_KOREAN_LATEX %%%

%%% TRANSLATION_NOTES %%%
[Significant decisions: ORIGINAL | TRANSLATION | RATIONALE]
%%% END_TRANSLATION_NOTES %%%

%%% GLOSSARY_UPDATES %%%
[New terms as JSON array: {{"english":...,"korean":...,"context_note":...}}]
%%% END_GLOSSARY_UPDATES %%%"""

# ── STEP 7: Korean xelatex Fix Loop ─────────────────────────────────────────

STEP7_SYS = r"""You are a LaTeX and Korean typography debugger. Fix ONLY xelatex compilation errors. Do NOT change any Korean text or math content.

Common xelatex+kotex issues to check:
- \usepackage{kotex} must be present
- \setmainfont{...} must reference an installed CJK font
- Remove \usepackage[T1]{fontenc} and \usepackage[utf8]{inputenc}
- Korean in math mode must use \text{한글}
- Ensure typography scope correctness: local \footnotesize/\small blocks must be followed by \normalsize.
- Preserve historical size contrast while preventing size leakage into main text.

Return corrected source wrapped as:
%%% CORRECTED_LATEX %%%
[full corrected source]
%%% END_CORRECTED_LATEX %%%
%%% CHANGES_MADE %%%
[numbered list: line number | error | fix applied]
%%% END_CHANGES %%%"""

# STEP 7 user prompt reuses STEP3_USR template
STEP7_USR = STEP3_USR
