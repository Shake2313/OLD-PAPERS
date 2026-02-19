#!/usr/bin/env python3
"""
PDF Digitization & Korean Translation Pipeline v2.0

Usage:
    python pipeline.py --input paper.pdf --name "PaperName" --output ./output
    python pipeline.py --input paper.pdf --name "PaperName" --output ./output --pages 1-3
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import sys
from datetime import datetime

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv()

from prompts import (
    STEP1_SYS, STEP1_USR,
    STEP2_SYS, STEP2_USR,
    STEP3_SYS, STEP3_USR,
    STEP5_SYS, STEP5_USR,
    STEP6_SYS, STEP6_USR,
    STEP7_SYS, STEP7_USR,
)
from steps import (
    pdf_to_images,
    get_pdf_page_count,
    call_vision,
    call_text,
    extract_block,
    normalize_latex_source,
    is_latex_document,
    auto_fix_loop,
    merge_pages,
    finalize_report,
)
from glossary_db import (
    get_db_path,
    init_db,
    fetch_terms_for_paper,
    upsert_terms,
)


def parse_page_range(pages_str: str, total: int) -> list[int]:
    """Parse page range string like '1-3' or '2,4,5' into 0-based indices."""
    indices = []
    for part in pages_str.split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-", 1)
            for i in range(int(start), int(end) + 1):
                indices.append(i - 1)
        else:
            indices.append(int(part) - 1)
    return [i for i in indices if 0 <= i < total]


def assess_rights(
    author: str | None,
    publication_year: int | None,
    death_year: int | None,
) -> dict:
    """Simple rights-check heuristic for logging."""
    current_year = datetime.now().year
    result = {
        "author": author,
        "publication_year": publication_year,
        "death_year": death_year,
        "assessment": "unknown",
        "reason": "Insufficient metadata.",
    }
    if publication_year is not None and publication_year <= 1929:
        result["assessment"] = "likely_public_domain_us"
        result["reason"] = "Publication year is 1929 or earlier (US heuristic)."
        return result
    if death_year is not None and current_year - death_year >= 70:
        result["assessment"] = "likely_public_domain_life_plus_70"
        result["reason"] = "Author death year is at least 70 years ago."
        return result
    if publication_year is not None:
        result["reason"] = "Publication year alone was not enough for this heuristic."
    return result


def build_rights_context(rights_info: dict) -> str:
    """Build a rights-context note to attach to model prompts."""
    assessment = rights_info.get("assessment", "unknown")
    reason = rights_info.get("reason", "")
    author = rights_info.get("author")
    publication_year = rights_info.get("publication_year")
    death_year = rights_info.get("death_year")
    summary = (
        "RIGHTS CONTEXT:\n"
        f"- author: {author}\n"
        f"- publication_year: {publication_year}\n"
        f"- death_year: {death_year}\n"
        f"- assessment: {assessment}\n"
        f"- reason: {reason}\n"
    )
    if assessment.startswith("likely_public_domain"):
        summary += (
            "The user states this source is public-domain and authorizes faithful transcription.\n"
            "For this task, treat verbatim transcription as permitted.\n"
        )
    else:
        summary += (
            "Rights status is not confirmed as public-domain by heuristic.\n"
            "Proceed conservatively if policy restrictions apply.\n"
        )
    return summary


def infer_metadata_from_structure(structure_json: str) -> tuple[str | None, int | None, int | None]:
    """Infer basic rights metadata from STEP 1 structure JSON."""
    try:
        data = json.loads(structure_json)
    except Exception:
        return None, None, None

    author_line = (
        data.get("article_header", {}).get("author_line")
        if isinstance(data, dict)
        else None
    )
    author = None
    if isinstance(author_line, str) and author_line.strip():
        author = re.sub(r"^(By|Von)\s+", "", author_line.strip(), flags=re.IGNORECASE)

    blob = json.dumps(data, ensure_ascii=False)
    years = re.findall(r"\b(1[6-9]\d{2}|20\d{2})\b", blob)
    publication_year = int(years[0]) if years else None

    known_death_years = {
        "emmy noether": 1935,
        "albert einstein": 1955,
    }
    death_year = known_death_years.get(author.lower()) if author else None
    return author, publication_year, death_year


def split_latex_into_page_docs(source: str) -> list[str]:
    """Split a merged LaTeX document into page-like mini documents by \\newpage."""
    m_begin = re.search(r"\\begin\{document\}", source)
    m_end = re.search(r"\\end\{document\}\s*$", source)
    if not m_begin or not m_end:
        return [source]
    preamble = source[: m_begin.end()]
    body = source[m_begin.end(): m_end.start()]
    chunks = [c.strip() for c in re.split(r"\n\s*\\newpage\s*\n", body) if c.strip()]
    if not chunks:
        return [source]
    return [f"{preamble}\n{chunk}\n\\end{{document}}" for chunk in chunks]


def chunked(items: list, size: int) -> list[list]:
    if size <= 0:
        return [items]
    return [items[i:i + size] for i in range(0, len(items), size)]


def run_pipeline(
    input_pdf: str,
    name: str,
    output_dir: str,
    pages: str | None = None,
    author: str | None = None,
    publication_year: int | None = None,
    death_year: int | None = None,
    workers: int = 4,
    resume: bool = True,
    translation_chunk_pages: int = 4,
):
    """Run the full STEP 0 ??8 pipeline."""
    glossary_db_path = get_db_path()
    init_db(glossary_db_path)
    os.makedirs(output_dir, exist_ok=True)
    images_dir = os.path.join(output_dir, "images")
    print("\n[RIGHTS] Running rights check heuristic...")
    meta_author = author
    meta_publication_year = publication_year
    meta_death_year = death_year

    rights_info = {
        "checked_at": datetime.now().isoformat(),
        **assess_rights(meta_author, meta_publication_year, meta_death_year),
    }
    rights_path = os.path.join(output_dir, f"{name}_rights_check.json")
    with open(rights_path, "w", encoding="utf-8") as f:
        json.dump(rights_info, f, ensure_ascii=False, indent=2)
    print(f"  Rights check saved: {rights_path}")
    print(f"  Assessment: {rights_info['assessment']} ({rights_info['reason']})")
    rights_context = build_rights_context(rights_info)

    # ?? STEP 0: PDF ??Images ????????????????????????????????????????????
    print("\n[STEP 0] Converting PDF to images...")
    page_numbers = None
    if pages:
        total_pages = get_pdf_page_count(input_pdf)
        selected = parse_page_range(pages, total_pages)
        page_numbers = selected
        print(f"  Total pages: {total_pages}")
        print(f"  Selected pages: {[i+1 for i in selected]}")
    image_paths = pdf_to_images(input_pdf, images_dir, page_numbers=page_numbers)
    total_pages = get_pdf_page_count(input_pdf) if pages else len(image_paths)
    if not pages:
        print(f"  Total pages: {total_pages}")

    num_pages = len(image_paths)

    # ?? STEP 1 & 2: Per-page structure analysis + LaTeX transcription ???
    page_latex_sources = [None] * num_pages
    all_transcription_notes = []

    def process_page(idx_img: tuple[int, str]) -> tuple[int, str, str | None]:
        idx, img_path = idx_img
        page_num = idx + 1
        struct_path = os.path.join(output_dir, f"page_{page_num:03d}_structure.json")
        page_tex_path = os.path.join(output_dir, f"page_{page_num:03d}.tex")

        if resume and os.path.exists(struct_path) and os.path.exists(page_tex_path):
            with open(page_tex_path, encoding="utf-8") as f:
                latex_source = normalize_latex_source(f.read())
            print(f"\n[STEP 1/2] Reusing cached page {page_num}/{num_pages}...")
            return idx, latex_source, None

        print(f"\n[STEP 1] Analyzing structure ??page {page_num}/{num_pages}...")
        structure_json = call_vision(STEP1_SYS, f"{rights_context}\n\n{STEP1_USR}", img_path)
        with open(struct_path, "w", encoding="utf-8") as f:
            f.write(structure_json)
        print(f"  Structure saved: {struct_path}")

        print(f"[STEP 2] Transcribing to LaTeX ??page {page_num}/{num_pages}...")
        step2_user = STEP2_USR.format(structure_analysis_json=structure_json)
        latex_source = None
        transcription_response = ""
        for attempt in range(1, 4):
            transcription_response = call_vision(
                STEP2_SYS,
                f"{rights_context}\n\n{step2_user}",
                img_path,
                max_tokens=16384,
            )
            candidate = extract_block(transcription_response, "BEGIN_LATEX")
            if not candidate:
                print(
                    f"  WARNING: Could not extract LaTeX for page {page_num} "
                    f"(attempt {attempt}/3)."
                )
                candidate = transcription_response
            candidate = normalize_latex_source(candidate)
            if is_latex_document(candidate):
                latex_source = candidate
                break
            print(
                f"  WARNING: Malformed LaTeX for page {page_num} "
                f"(attempt {attempt}/3). Retrying..."
            )
        if latex_source is None:
            raise RuntimeError(
                f"STEP 2 failed: malformed LaTeX on page {page_num} after 3 attempts."
            )
        with open(page_tex_path, "w", encoding="utf-8") as f:
            f.write(latex_source)
        print(f"  LaTeX saved: {page_tex_path}")
        notes = extract_block(transcription_response, "TRANSCRIPTION_NOTES")
        return idx, latex_source, notes

    first_idx, first_latex, first_notes = process_page((0, image_paths[0]))
    page_latex_sources[first_idx] = first_latex
    if first_notes:
        all_transcription_notes.append(f"--- Page {first_idx + 1} ---\n{first_notes}")

    if rights_info.get("assessment") == "unknown":
        first_struct_path = os.path.join(output_dir, "page_001_structure.json")
        if os.path.exists(first_struct_path):
            with open(first_struct_path, encoding="utf-8") as f:
                first_structure_json = f.read()
            inf_author, inf_pub_year, inf_death_year = infer_metadata_from_structure(first_structure_json)
            if meta_author is None and inf_author:
                meta_author = inf_author
            if meta_publication_year is None and inf_pub_year:
                meta_publication_year = inf_pub_year
            if meta_death_year is None and inf_death_year:
                meta_death_year = inf_death_year
            if any(v is not None for v in [inf_author, inf_pub_year, inf_death_year]):
                rights_info = {
                    "checked_at": datetime.now().isoformat(),
                    **assess_rights(meta_author, meta_publication_year, meta_death_year),
                }
                with open(rights_path, "w", encoding="utf-8") as f:
                    json.dump(rights_info, f, ensure_ascii=False, indent=2)
                rights_context = build_rights_context(rights_info)
                print("  [RIGHTS] Updated from inferred metadata on page 1.")
                print(
                    f"  [RIGHTS] Assessment: {rights_info['assessment']} "
                    f"({rights_info['reason']})"
                )

    remaining = list(enumerate(image_paths))[1:]
    max_workers = max(1, min(workers, len(remaining))) if remaining else 1
    if remaining and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_page, item) for item in remaining]
            for future in as_completed(futures):
                idx, latex_source, notes = future.result()
                page_latex_sources[idx] = latex_source
                if notes:
                    all_transcription_notes.append(f"--- Page {idx + 1} ---\n{notes}")
    elif remaining:
        for item in remaining:
            idx, latex_source, notes = process_page(item)
            page_latex_sources[idx] = latex_source
            if notes:
                all_transcription_notes.append(f"--- Page {idx + 1} ---\n{notes}")

    page_latex_sources = [src for src in page_latex_sources if src]
    # Save transcription notes
    if all_transcription_notes:
        notes_path = os.path.join(output_dir, f"{name}_transcription_notes.txt")
        with open(notes_path, "w", encoding="utf-8") as f:
            f.write("\n\n".join(all_transcription_notes))

    # ?? Merge pages ?????????????????????????????????????????????????????
    print(f"\n[MERGE] Merging {num_pages} page(s)...")
    merged_latex = merge_pages(page_latex_sources)
    if not is_latex_document(merged_latex):
        raise RuntimeError("Merged LaTeX is malformed. Aborting before compilation.")
    merged_tex_path = os.path.join(output_dir, f"{name}_merged.tex")
    with open(merged_tex_path, "w", encoding="utf-8") as f:
        f.write(merged_latex)
    print(f"  Merged LaTeX saved: {merged_tex_path}")

    # ?? STEP 3: Compile + auto-fix (pdflatex) ??????????????????????????
    final_dig_tex = os.path.join(output_dir, f"{name}_digitalized.tex")
    dig_pdf = os.path.join(output_dir, f"{name}_digitalized.pdf")
    dig_err_log = os.path.join(output_dir, f"{name}_digitalized_error.log")
    if resume and os.path.exists(final_dig_tex) and os.path.exists(dig_pdf) and not os.path.exists(dig_err_log):
        print("\n[STEP 3] Reusing cached digitalized PDF/TeX...")
        with open(final_dig_tex, encoding="utf-8") as f:
            final_dig_latex = f.read()
        dig_ok = True
    else:
        if os.path.exists(dig_err_log):
            print("\n[STEP 3] Found previous error log; recompiling digitalized PDF...")
        print("\n[STEP 3] Compiling digitalized PDF (pdflatex)...")
        dig_ok, final_dig_latex, dig_pdf = auto_fix_loop(
            merged_latex,
            output_dir,
            f"{name}_digitalized",
            max_attempts=5,
            compiler="pdflatex",
            fix_system_prompt=STEP3_SYS,
            fix_user_template=STEP3_USR,
            double_compile=False,
        )

    # Save final digitalized LaTeX
    with open(final_dig_tex, "w", encoding="utf-8") as f:
        f.write(final_dig_latex)

    if dig_ok:
        print(f"  [STEP 4] Digitalized PDF: {dig_pdf}")
    else:
        print("  [STEP 4] WARNING: Digitalized PDF compilation failed.")

    # ?? STEP 5: Glossary ????????????????????????????????????????????????
    glossary = []
    if resume:
        glossary = fetch_terms_for_paper(glossary_db_path, name)
        if glossary:
            print("\n[STEP 5] Reusing cached glossary from DB...")

    if not glossary:
        print("\n[STEP 5] Building glossary...")
        step5_user = STEP5_USR.format(digitalized_latex_source=final_dig_latex)
        glossary_response = call_text(STEP5_SYS, step5_user)

        # Parse glossary JSON
        try:
            glossary = json.loads(glossary_response)
        except json.JSONDecodeError:
            # Try to extract JSON array from response
            m = re.search(r"\[.*\]", glossary_response, re.DOTALL)
            if m:
                try:
                    glossary = json.loads(m.group())
                except json.JSONDecodeError:
                    print("  WARNING: Could not parse glossary JSON.")

        upsert_terms(glossary_db_path, glossary, source_paper=name)
    print(f"  Glossary saved to DB: {glossary_db_path} ({len(glossary)} terms)")

    korean_tex_path = os.path.join(output_dir, f"{name}_Korean.tex")
    tnotes_path = os.path.join(output_dir, f"{name}_translation_notes.txt")
    if resume and os.path.exists(korean_tex_path):
        print("\n[STEP 6] Reusing cached Korean LaTeX...")
        with open(korean_tex_path, encoding="utf-8") as f:
            korean_latex = f.read()
    else:
        print("\n[STEP 6] Translating to Korean...")
        glossary_json_str = json.dumps(glossary, ensure_ascii=False, indent=2)
        page_docs = split_latex_into_page_docs(final_dig_latex)
        groups = chunked(page_docs, translation_chunk_pages) if len(page_docs) > translation_chunk_pages else [page_docs]

        translated_docs = []
        notes_all = []
        for chunk_idx, docs in enumerate(groups, start=1):
            chunk_source = merge_pages(docs)
            print(f"  [STEP 6] Translating chunk {chunk_idx}/{len(groups)}...")
            step6_user = STEP6_USR.format(
                glossary_json=glossary_json_str,
                digitalized_latex_source=chunk_source,
            )
            translation_response = call_text(STEP6_SYS, step6_user, max_tokens=16384)
            chunk_korean = extract_block(translation_response, "BEGIN_KOREAN_LATEX")
            if not chunk_korean:
                print("  WARNING: Could not extract Korean LaTeX chunk. Using raw response.")
                chunk_korean = translation_response
            chunk_korean = normalize_latex_source(chunk_korean)
            translated_docs.append(chunk_korean)

            glossary_updates_str = extract_block(translation_response, "GLOSSARY_UPDATES")
            if glossary_updates_str:
                try:
                    updates = json.loads(glossary_updates_str)
                    if isinstance(updates, list):
                        glossary.extend(updates)
                except json.JSONDecodeError:
                    pass

            translation_notes = extract_block(translation_response, "TRANSLATION_NOTES")
            if translation_notes:
                notes_all.append(f"--- Chunk {chunk_idx} ---\n{translation_notes}")

        korean_latex = merge_pages(translated_docs) if len(translated_docs) > 1 else translated_docs[0]
        with open(korean_tex_path, "w", encoding="utf-8") as f:
            f.write(korean_latex)
        upsert_terms(glossary_db_path, glossary, source_paper=name)
        if notes_all:
            with open(tnotes_path, "w", encoding="utf-8") as f:
                f.write("\n\n".join(notes_all))
    print(f"  Korean LaTeX saved: {korean_tex_path}")

    # ?? STEP 7: Compile Korean PDF (xelatex) + auto-fix ?????????????????
    kor_pdf = os.path.join(output_dir, f"{name}_Korean.pdf")
    kor_err_log = os.path.join(output_dir, f"{name}_Korean_error.log")
    if resume and os.path.exists(kor_pdf) and not os.path.exists(kor_err_log):
        print("\n[STEP 7] Reusing cached Korean PDF...")
        kor_ok = True
        final_kor_latex = korean_latex
    else:
        if os.path.exists(kor_err_log):
            print("\n[STEP 7] Found previous error log; recompiling Korean PDF...")
        print("\n[STEP 7] Compiling Korean PDF (xelatex)...")
        kor_ok, final_kor_latex, kor_pdf = auto_fix_loop(
            korean_latex,
            output_dir,
            f"{name}_Korean",
            max_attempts=5,
            compiler="xelatex",
            fix_system_prompt=STEP7_SYS,
            fix_user_template=STEP7_USR,
            double_compile=False,
        )

    # Save final Korean LaTeX
    with open(korean_tex_path, "w", encoding="utf-8") as f:
        f.write(final_kor_latex)

    if kor_ok:
        print(f"  Korean PDF: {kor_pdf}")
    else:
        print("  WARNING: Korean PDF compilation failed.")

    # ?? STEP 8: Quality report ??????????????????????????????????????????
    print("\n[STEP 8] Generating quality report...")
    finalize_report(
        name,
        num_pages,
        dig_ok,
        kor_ok,
        len(glossary),
        output_dir,
        glossary_db_path=glossary_db_path,
    )

    # ?? Summary ?????????????????????????????????????????????????????????
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Pages processed:    {num_pages}")
    print(f"  Digitalized PDF:    {'OK' if dig_ok else 'FAILED'}")
    print(f"  Korean PDF:         {'OK' if kor_ok else 'FAILED'}")
    print(f"  Glossary terms:     {len(glossary)}")
    print(f"  Output directory:   {output_dir}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="PDF Digitization & Korean Translation Pipeline v2.0"
    )
    parser.add_argument("--input", required=True, help="Path to input PDF file")
    parser.add_argument("--name", required=True, help="Paper name (used for output filenames)")
    parser.add_argument("--output", default="./output", help="Output directory (default: ./output)")
    parser.add_argument("--pages", default=None, help="Page range, e.g. '1-3' or '1,3,5'")
    parser.add_argument("--author", default=None, help="Author name for rights check log")
    parser.add_argument("--publication-year", type=int, default=None, help="Publication year for rights check log")
    parser.add_argument("--death-year", type=int, default=None, help="Author death year for rights check log")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers for page-level STEP 1/2")
    parser.add_argument("--no-resume", action="store_true", help="Disable cache/resume and recompute all steps")
    parser.add_argument("--translation-chunk-pages", type=int, default=4, help="Pages per translation chunk in STEP 6")

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)

    run_pipeline(
        args.input,
        args.name,
        args.output,
        args.pages,
        args.author,
        args.publication_year,
        args.death_year,
        args.workers,
        not args.no_resume,
        args.translation_chunk_pages,
    )


if __name__ == "__main__":
    main()

