"""
Core pipeline functions for PDF digitization & Korean translation.
"""

import base64
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime

from google import genai
from google.genai import types

from prompts import STEP3_SYS, STEP3_USR

# Model to use for all API calls — override via environment variable MODEL_NAME
DEFAULT_MODEL = os.environ.get("MODEL_NAME", "gemini-3-flash-preview")
API_TIMEOUT_SEC = int(os.environ.get("API_TIMEOUT_SEC", "180"))
_GENAI_CLIENT: genai.Client | None = None


def _ensure_genai_configured() -> genai.Client:
    """Create a GenAI client once and reuse it."""
    global _GENAI_CLIENT
    if _GENAI_CLIENT is not None:
        return _GENAI_CLIENT
    api_key = (
        os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
        or os.environ.get("GOOGLE_GEMINI_API_KEY")
    )
    if api_key:
        _GENAI_CLIENT = genai.Client(api_key=api_key)
    else:
        # Allow Vertex AI env-based configuration if present.
        _GENAI_CLIENT = genai.Client()
    return _GENAI_CLIENT


def _block_none_safety_settings():
    """Return safety settings with BLOCK_NONE across categories."""
    return [
        types.SafetySetting(
            category="HARM_CATEGORY_HARASSMENT",
            threshold="BLOCK_NONE",
        ),
        types.SafetySetting(
            category="HARM_CATEGORY_HATE_SPEECH",
            threshold="BLOCK_NONE",
        ),
        types.SafetySetting(
            category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
            threshold="BLOCK_NONE",
        ),
        types.SafetySetting(
            category="HARM_CATEGORY_DANGEROUS_CONTENT",
            threshold="BLOCK_NONE",
        ),
    ]


def _find_poppler_path() -> str | None:
    """Auto-detect poppler bin directory on Windows."""
    import shutil
    if shutil.which("pdftoppm"):
        return None  # already on PATH
    candidates = [
        os.path.expanduser("~/poppler/poppler-24.08.0/Library/bin"),
        os.path.expanduser("~/poppler/Library/bin"),
        "C:/poppler/poppler-24.08.0/Library/bin",
    ]
    for p in candidates:
        if os.path.isfile(os.path.join(p, "pdftoppm.exe")) or os.path.isfile(os.path.join(p, "pdftoppm")):
            return p
    return None


def _ensure_latex_on_path():
    """Add MiKTeX to PATH if not already there."""
    import shutil
    if shutil.which("pdflatex"):
        return
    miktex_bin = os.path.expanduser("~/AppData/Local/Programs/MiKTeX/miktex/bin/x64")
    if os.path.isdir(miktex_bin):
        os.environ["PATH"] = miktex_bin + os.pathsep + os.environ.get("PATH", "")


def get_pdf_page_count(pdf_path: str) -> int:
    """Return total page count without rendering all pages."""
    import fitz

    doc = fitz.open(pdf_path)
    count = doc.page_count
    doc.close()
    return count


def pdf_to_images(
    pdf_path: str,
    output_dir: str,
    dpi: int = 400,
    page_numbers: list[int] | None = None,
) -> list[str]:
    """STEP 0: Convert PDF pages to PNG images.

    page_numbers is 0-based and, if provided, only those pages are rendered.
    """
    from pdf2image import convert_from_path
    from pdf2image.exceptions import PDFInfoNotInstalledError

    os.makedirs(output_dir, exist_ok=True)
    if page_numbers is not None:
        # Render only selected pages via PyMuPDF (fast for sparse pages).
        import fitz

        selected = [p for p in page_numbers if p >= 0]
        if not selected:
            return []
        doc = fitz.open(pdf_path)
        scale = dpi / 72.0
        mat = fitz.Matrix(scale, scale)
        paths = []
        for page_idx in selected:
            if page_idx >= doc.page_count:
                continue
            page = doc.load_page(page_idx)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            p = os.path.join(output_dir, f"page_{page_idx+1:03d}.png")
            pix.save(p)
            paths.append(p)
            print(
                f"  [STEP 0] Saved page {page_idx+1}/{doc.page_count}: {p} (pymupdf)"
            )
        doc.close()
        return paths

    poppler_path = _find_poppler_path()
    kwargs = {"poppler_path": poppler_path} if poppler_path else {}
    try:
        images = convert_from_path(pdf_path, dpi=dpi, **kwargs)
        paths = []
        for i, img in enumerate(images):
            p = os.path.join(output_dir, f"page_{i+1:03d}.png")
            img.save(p, "PNG")
            paths.append(p)
            print(f"  [STEP 0] Saved page {i+1}/{len(images)}: {p}")
        return paths
    except PDFInfoNotInstalledError:
        # Fallback for environments without Poppler: render via PyMuPDF.
        import fitz

        doc = fitz.open(pdf_path)
        scale = dpi / 72.0
        mat = fitz.Matrix(scale, scale)
        paths = []
        for i, page in enumerate(doc):
            pix = page.get_pixmap(matrix=mat, alpha=False)
            p = os.path.join(output_dir, f"page_{i+1:03d}.png")
            pix.save(p)
            paths.append(p)
            print(f"  [STEP 0] Saved page {i+1}/{len(doc)}: {p} (pymupdf)")
        doc.close()
        return paths


def image_to_base64(path: str) -> str:
    """Read an image file and return its base64-encoded string."""
    with open(path, "rb") as f:
        return base64.standard_b64encode(f.read()).decode("utf-8")


def _media_type(path: str) -> str:
    ext = Path(path).suffix.lower()
    return {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg"}.get(
        ext.lstrip("."), "image/png"
    )


def _extract_text(response) -> str:
    """Best-effort text extraction from GenAI response."""
    try:
        if hasattr(response, "text") and response.text:
            return response.text
    except Exception:
        pass

    try:
        candidate = response.candidates[0]
        finish_reason = getattr(candidate, "finish_reason", None)
        if str(finish_reason) == "FinishReason.RECITATION":
            raise RuntimeError(
                "Gemini blocked output (finish_reason=RECITATION: potential copyrighted recitation). "
                "This pipeline asks for near-verbatim transcription, so this can happen. "
                "Try another model/provider or use a non-verbatim OCR path."
            )
    except RuntimeError:
        raise
    except Exception:
        pass

    try:
        parts = response.candidates[0].content.parts
        return "".join(getattr(p, "text", "") for p in parts).strip()
    except Exception:
        return ""


def call_vision(
    sys_prompt: str, user_prompt: str, img_path: str, max_tokens: int = 8192
) -> str:
    """Call Gemini with an image + text prompt."""
    client = _ensure_genai_configured()
    with open(img_path, "rb") as f:
        img_part = types.Part.from_bytes(
            data=f.read(),
            mime_type=_media_type(img_path),
        )
    resp = client.models.generate_content(
        model=DEFAULT_MODEL,
        contents=[user_prompt, img_part],
        config=types.GenerateContentConfig(
            system_instruction=sys_prompt,
            max_output_tokens=max_tokens,
            safety_settings=_block_none_safety_settings(),
        ),
    )
    return _extract_text(resp)


def call_text(sys_prompt: str, user_prompt: str, max_tokens: int = 8192) -> str:
    """Call Gemini text API (no image)."""
    client = _ensure_genai_configured()
    resp = client.models.generate_content(
        model=DEFAULT_MODEL,
        contents=user_prompt,
        config=types.GenerateContentConfig(
            system_instruction=sys_prompt,
            max_output_tokens=max_tokens,
            safety_settings=_block_none_safety_settings(),
        ),
    )
    return _extract_text(resp)


def extract_block(text: str, tag: str) -> str | None:
    """Extract content between %%% TAG %%% and %%% END_TAG %%% markers.

    Handles both patterns:
      %%% BEGIN_LATEX %%% ... %%% END_LATEX %%%
      %%% CORRECTED_LATEX %%% ... %%% END_CORRECTED_LATEX %%%
    """
    # Derive end tag: BEGIN_X -> END_X, otherwise END_TAG
    if tag.startswith("BEGIN_"):
        end_tag = "END_" + tag[len("BEGIN_"):]
    else:
        end_tag = "END_" + tag
    pattern = rf"%%%\s*{re.escape(tag)}\s*%%%(.*?)%%%\s*{re.escape(end_tag)}\s*%%%"
    m = re.search(pattern, text, re.DOTALL)
    if m:
        return m.group(1).strip()

    # Fallback: BEGIN tag exists but END tag is missing (truncated response).
    begin_pattern = rf"%%%\s*{re.escape(tag)}\s*%%%"
    b = re.search(begin_pattern, text, re.DOTALL)
    if b:
        return text[b.end():].strip()

    # Fallback: fenced LaTeX block.
    fence = re.search(r"```(?:latex)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if fence:
        return fence.group(1).strip()
    return None


def normalize_latex_source(text: str) -> str:
    """Normalize model output into a compilable LaTeX document when possible."""
    src = text.strip()
    src = re.sub(r"^%%%\s*BEGIN_[A-Z_]+\s*%%%[\r\n]*", "", src)
    src = re.sub(r"[\r\n]*%%%\s*END_[A-Z_]+\s*%%%$", "", src)
    src = re.sub(r"^```(?:latex)?\s*", "", src, flags=re.IGNORECASE)
    src = re.sub(r"\s*```$", "", src)
    if "\\begin{document}" in src and "\\end{document}" not in src:
        src += "\n\n\\end{document}\n"
    return src


def _looks_like_latex_document(source: str) -> bool:
    src = source.strip()
    return "\\documentclass" in src or "\\begin{document}" in src


def is_latex_document(source: str) -> bool:
    """Public validator used by pipeline guards."""
    return _looks_like_latex_document(source)


def _is_plausible_fix(old_source: str, new_source: str) -> bool:
    """Reject model 'fixes' that likely dropped substantial content."""
    old_len = len(old_source.strip())
    new_len = len(new_source.strip())
    if old_len == 0:
        return False
    # Compile fixes should not remove large portions of content.
    if new_len < int(old_len * 0.9):
        return False
    return True


def _apply_common_compile_fix(source: str, error_log: str, compiler: str) -> str | None:
    """Apply deterministic fixes for common compiler/runtime issues."""
    if (
        compiler == "pdflatex"
        and "font expansion" in error_log.lower()
        and "\\microtypesetup{expansion=false}" not in source
    ):
        # Disable expansion when non-scalable fonts trigger pdfTeX microtype errors.
        if "microtype" in source:
            package_line = re.search(r"(\\usepackage\{[^}]*microtype[^}]*\})", source)
            if package_line:
                return source.replace(
                    package_line.group(1),
                    package_line.group(1) + "\n\\microtypesetup{expansion=false}",
                    1,
                )
            if "\\begin{document}" in source:
                return source.replace(
                    "\\begin{document}",
                    "\\microtypesetup{expansion=false}\n\\begin{document}",
                    1,
                )
    if "missing $ inserted" in error_log.lower() and "\\cdot" in source:
        # Common OCR/LLM artifact: \cdot emitted in text mode (often in fancyfoot).
        fixed = re.sub(
            r"\\cdot(?!\s*\$)",
            r"$\\cdot$",
            source,
        )
        if fixed != source:
            return fixed
    if compiler == "xelatex" and "fontspec" in error_log.lower() and "cannot be found" in error_log.lower():
        # Replace unavailable font with a common Windows Korean font fallback.
        if "\\setmainfont{" in source:
            return re.sub(
                r"\\setmainfont\{[^}]+\}",
                r"\\setmainfont{Malgun Gothic}",
                source,
                count=1,
            )
        # If no explicit main font exists, add one after kotex.
        if "\\usepackage{kotex}" in source and "\\setmainfont{" not in source:
            return source.replace("\\usepackage{kotex}", "\\usepackage{kotex}\n\\setmainfont{Malgun Gothic}")
    return None


def compile_latex(
    source: str,
    output_dir: str,
    filename: str,
    compiler: str = "pdflatex",
) -> tuple[bool, str, str]:
    """
    Compile LaTeX source to PDF.
    Returns (success, pdf_path, error_log).
    """
    _ensure_latex_on_path()
    os.makedirs(output_dir, exist_ok=True)
    tex_path = os.path.join(output_dir, f"{filename}.tex")
    with open(tex_path, "w", encoding="utf-8") as f:
        f.write(source)

    cmd = [
        compiler,
        "-interaction=nonstopmode",
        "-halt-on-error",
        f"-output-directory={output_dir}",
        tex_path,
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120, encoding="utf-8",
            errors="replace",
        )
        log_path = os.path.join(output_dir, f"{filename}.log")
        error_log = ""
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8", errors="replace") as lf:
                error_log = lf.read()

        pdf_path = os.path.join(output_dir, f"{filename}.pdf")
        success = result.returncode == 0 and os.path.exists(pdf_path)
        return success, pdf_path, error_log
    except subprocess.TimeoutExpired:
        return False, "", "Compilation timed out after 120 seconds."
    except FileNotFoundError:
        return False, "", f"Compiler '{compiler}' not found. Is it installed?"


def auto_fix_loop(
    source: str,
    output_dir: str,
    filename: str,
    max_attempts: int = 5,
    compiler: str = "pdflatex",
    fix_system_prompt: str = STEP3_SYS,
    fix_user_template: str = STEP3_USR,
    double_compile: bool = False,
) -> tuple[bool, str, str]:
    """
    STEP 3/7: Compile LaTeX, and if it fails, ask Gemini to fix errors.
    Returns (success, final_source, pdf_path).
    """
    current_source = normalize_latex_source(source)

    for attempt in range(1, max_attempts + 1):
        print(f"  [COMPILE] Attempt {attempt}/{max_attempts} ({compiler})...")
        success, pdf_path, error_log = compile_latex(
            current_source, output_dir, filename, compiler
        )

        if success:
            print(f"  [COMPILE] Success on attempt {attempt}.")
            if double_compile:
                # Double-compile for cross-references (optional)
                compile_latex(current_source, output_dir, filename, compiler)
            # Clear any previous error log on success.
            err_log_path = os.path.join(output_dir, f"{filename}_error.log")
            if os.path.exists(err_log_path):
                try:
                    os.remove(err_log_path)
                except OSError:
                    pass
            pdf_path = os.path.join(output_dir, f"{filename}.pdf")
            return True, current_source, pdf_path

        deterministic = _apply_common_compile_fix(current_source, error_log, compiler)
        if deterministic:
            print("  [COMPILE] Applied deterministic fix and retrying...")
            current_source = deterministic
            continue

        print(f"  [COMPILE] Failed attempt {attempt}. Requesting fix...")

        # Extract last 200 lines of error log for context
        log_lines = error_log.split("\n")
        truncated_log = "\n".join(log_lines[-200:]) if len(log_lines) > 200 else error_log

        user_prompt = fix_user_template.format(
            attempt_number=attempt,
            latex_source=current_source,
            error_log=truncated_log,
        )

        fix_response = call_text(fix_system_prompt, user_prompt)
        corrected = extract_block(fix_response, "CORRECTED_LATEX")
        if corrected:
            normalized = normalize_latex_source(corrected)
            if _looks_like_latex_document(normalized):
                if _is_plausible_fix(current_source, normalized):
                    current_source = normalized
                else:
                    print("  [COMPILE] Warning: Ignored over-aggressive fix that removed content.")
            else:
                print("  [COMPILE] Warning: Ignored malformed corrected LaTeX from model.")
        else:
            print("  [COMPILE] Warning: Could not extract corrected LaTeX from response.")

    # Save error log on final failure
    err_log_path = os.path.join(output_dir, f"{filename}_error.log")
    with open(err_log_path, "w", encoding="utf-8") as f:
        f.write(error_log)
    print(f"  [COMPILE] All {max_attempts} attempts failed. Error log: {err_log_path}")
    return False, current_source, ""


def merge_pages(latex_pages: list[str]) -> str:
    """
    Merge multiple single-page LaTeX documents into one.
    Uses preamble from first page, body from all pages joined with \\newpage.
    """
    if len(latex_pages) == 1:
        return latex_pages[0]

    def split_doc(src: str) -> tuple[str, str]:
        """Split into (preamble, body) at \\begin{document}."""
        m = re.search(r"\\begin\{document\}", src)
        if not m:
            return src, ""
        preamble = src[: m.end()]
        rest = src[m.end() :]
        # Remove \end{document}
        rest = re.sub(r"\\end\{document\}\s*$", "", rest).strip()
        return preamble, rest

    preamble, first_body = split_doc(latex_pages[0])
    bodies = [first_body]

    for page_src in latex_pages[1:]:
        _, body = split_doc(page_src)
        bodies.append(body)

    merged_body = "\n\n\\newpage\n\n".join(bodies)
    return f"{preamble}\n{merged_body}\n\\end{{document}}"


def finalize_report(
    name: str,
    num_pages: int,
    digitalized_ok: bool,
    korean_ok: bool,
    glossary_count: int,
    output_dir: str,
    glossary_db_path: str | None = None,
) -> str:
    """STEP 8: Generate quality report JSON."""
    report = {
        "paper_name": name,
        "date": datetime.now().isoformat(),
        "total_pages": num_pages,
        "digitalized_pdf": {
            "compiled": digitalized_ok,
            "file": f"{name}_digitalized.pdf" if digitalized_ok else None,
        },
        "korean_pdf": {
            "compiled": korean_ok,
            "file": f"{name}_Korean.pdf" if korean_ok else None,
        },
        "glossary": {
            "term_count": glossary_count,
            "source": "db",
            "db_path": glossary_db_path,
        },
    }
    report_path = os.path.join(output_dir, f"{name}_quality_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  [STEP 8] Quality report saved: {report_path}")
    return report_path
