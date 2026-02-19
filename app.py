"""
논문 디지털화 & 한국어 번역 파이프라인 — Streamlit UI
Run:  streamlit run app.py
"""

import json
import base64
import os
import sys
import tempfile
import threading
import time
import queue
from pathlib import Path
from datetime import datetime

import streamlit as st

from glossary_db import get_db_path, init_db, fetch_terms_for_paper
from steps import compile_latex

# ── 페이지 설정 ─────────────────────────────────────────────────
st.set_page_config(page_title="논문 파이프라인", page_icon="📜", layout="wide")

# ── 세션 상태 초기화 ────────────────────────────────────────────
for key, default in {
    "pipeline_running": False,
    "pipeline_done": False,
    "pipeline_log": [],
    "pipeline_error": None,
    "output_dir": None,
    "pipeline_event_queue": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


def _drain_pipeline_events():
    q = st.session_state.get("pipeline_event_queue")
    if q is None:
        return
    while True:
        try:
            event_type, payload = q.get_nowait()
        except queue.Empty:
            break
        if event_type == "log":
            st.session_state["pipeline_log"].append(payload)
        elif event_type == "error":
            st.session_state["pipeline_error"] = payload
        elif event_type == "done":
            st.session_state["pipeline_running"] = False
            st.session_state["pipeline_done"] = True
            st.session_state["pipeline_event_queue"] = None

# ── 유틸 함수 ───────────────────────────────────────────────────
def read_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def pdf_iframe(path: Path):
    data = path.read_bytes()
    b64 = base64.b64encode(data).decode()
    html = (
        f'<iframe src="data:application/pdf;base64,{b64}" '
        f'width="100%" height="700" type="application/pdf"></iframe>'
    )
    st.markdown(html, unsafe_allow_html=True)


def download_btn(path: Path, label: str):
    st.download_button(label, data=path.read_bytes(), file_name=path.name)


_drain_pipeline_events()


def _run_pipeline_thread(
    pdf_bytes: bytes,
    paper_name: str,
    output_dir: str,
    pages: str | None,
    author: str | None,
    publication_year: int | None,
    death_year: int | None,
    event_queue,
):
    """파이프라인을 별도 스레드에서 실행하며 로그를 session_state에 기록."""
    import io
    from contextlib import redirect_stdout, redirect_stderr

    # PDF를 임시 파일로 저장
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp.write(pdf_bytes)
    tmp.close()

    log_buf = io.StringIO()
    event_queue.put(("log", "[THREAD] Pipeline thread started"))
    try:
        # pipeline 모듈이 같은 폴더에 있으므로 sys.path에 추가
        project_dir = str(Path(__file__).parent)
        if project_dir not in sys.path:
            sys.path.insert(0, project_dir)

        from pipeline import run_pipeline

        # stdout/stderr를 캡처하면서 session_state 로그에도 실시간 추가
        class LogCapture:
            def __init__(self, buf):
                self.buf = buf
            def write(self, s):
                if s.strip():
                    self.buf.write(s)
                    event_queue.put(("log", s.rstrip()))
            def flush(self):
                pass
            def reconfigure(self, **kwargs):
                # Called by pipeline.py on Windows; no-op for this capture stream.
                return self

        capture = LogCapture(log_buf)
        with redirect_stdout(capture), redirect_stderr(capture):
            run_pipeline(
                tmp.name,
                paper_name,
                output_dir,
                pages,
                author=author,
                publication_year=publication_year,
                death_year=death_year,
            )

        event_queue.put(("error", None))
    except Exception as e:
        event_queue.put(("error", str(e)))
    finally:
        os.unlink(tmp.name)
        event_queue.put(("done", None))


# ── 사이드바: 파일 업로드 & 파이프라인 실행 ──────────────────────
with st.sidebar:
    st.header("논문 업로드")
    uploaded = st.file_uploader(
        "PDF 파일을 드래그하거나 클릭하여 업로드",
        type=["pdf"],
        accept_multiple_files=False,
    )

    paper_name = st.text_input(
        "논문 이름 (출력 파일명에 사용)",
        value=uploaded.name.rsplit(".", 1)[0] if uploaded else "",
        help="예: Einstein_1905",
    )

    # 출력 폴더 — 기본값은 프로젝트 내 output/<논문이름>
    default_out = str(Path(__file__).parent / "output" / paper_name) if paper_name else ""
    output_dir_input = st.text_input("출력 폴더", value=default_out)

    pages_input = st.text_input(
        "처리할 페이지 (선택사항)",
        placeholder="예: 1-3  또는  1,3,5",
        help="비워두면 전체 페이지를 처리합니다.",
    )

    st.caption("Rights Check Metadata (optional)")
    author_input = st.text_input("Author", value="Emmy Noether")
    publication_year_input = st.text_input("Publication year", value="1918")
    death_year_input = st.text_input("Author death year", value="1935")
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        run_btn = st.button(
            "파이프라인 실행",
            type="primary",
            disabled=not uploaded or not paper_name or st.session_state["pipeline_running"],
            width="stretch",
        )
    with col2:
        if st.button("초기화", width="stretch"):
            st.session_state["pipeline_running"] = False
            st.session_state["pipeline_done"] = False
            st.session_state["pipeline_log"] = []
            st.session_state["pipeline_error"] = None
            st.session_state["output_dir"] = None
            st.session_state["pipeline_event_queue"] = None
            st.rerun()

    # 파이프라인 실행 트리거
    if run_btn and uploaded and paper_name:
        st.session_state["pipeline_running"] = True
        st.session_state["pipeline_done"] = False
        st.session_state["pipeline_log"] = []
        st.session_state["pipeline_error"] = None
        st.session_state["output_dir"] = output_dir_input

        pdf_bytes = uploaded.getvalue()
        pages = pages_input.strip() or None
        author = author_input.strip() or None
        try:
            publication_year = int(publication_year_input.strip()) if publication_year_input.strip() else None
        except ValueError:
            publication_year = None
        try:
            death_year = int(death_year_input.strip()) if death_year_input.strip() else None
        except ValueError:
            death_year = None
        event_queue = queue.Queue()
        st.session_state["pipeline_event_queue"] = event_queue

        thread = threading.Thread(
            target=_run_pipeline_thread,
            args=(
                pdf_bytes,
                paper_name,
                output_dir_input,
                pages,
                author,
                publication_year,
                death_year,
                event_queue,
            ),
            daemon=True,
        )
        thread.start()
        st.rerun()

    # 진행 상태 표시
    if st.session_state["pipeline_running"]:
        st.info("파이프라인 실행 중...")
        st.spinner("처리 중")

    if st.session_state["pipeline_error"]:
        st.error(f"오류 발생: {st.session_state['pipeline_error']}")

    if st.session_state["pipeline_done"] and not st.session_state["pipeline_error"]:
        st.success("파이프라인 완료!")

    # 기존 결과 폴더 불러오기
    st.divider()
    st.subheader("기존 결과 보기")
    existing_dir = st.text_input(
        "결과 폴더 경로",
        value=str(Path(__file__).parent / "test_output"),
        key="existing_dir",
    )
    if st.button("불러오기", width="stretch"):
        if Path(existing_dir).is_dir():
            st.session_state["output_dir"] = existing_dir
            st.session_state["pipeline_done"] = True
            st.session_state["pipeline_error"] = None
            st.rerun()
        else:
            st.error("폴더를 찾을 수 없습니다.")


# ── 메인 영역 ───────────────────────────────────────────────────
st.title("📜 논문 디지털화 & 한국어 번역")

# ── 실행 로그 (실행 중이거나 완료 후) ────────────────────────────
if st.session_state["pipeline_running"] or st.session_state["pipeline_log"]:
    with st.expander("실행 로그", expanded=st.session_state["pipeline_running"]):
        log_text = "\n".join(st.session_state["pipeline_log"][-100:])
        st.code(log_text, language="text")
        if st.session_state["pipeline_running"]:
            time.sleep(2)
            st.rerun()

# ── 결과가 없으면 안내 메시지 ────────────────────────────────────
output_dir = st.session_state.get("output_dir")
if not output_dir or not Path(output_dir).is_dir():
    st.info("왼쪽 사이드바에서 PDF를 업로드하고 파이프라인을 실행하거나, 기존 결과 폴더를 불러오세요.")
    st.stop()

output_path = Path(output_dir)

# ── 품질 보고서 요약 ────────────────────────────────────────────
report_files = sorted(output_path.glob("*_quality_report.json"))
report = None
if report_files:
    report = read_json(report_files[0])
    pname = report.get("paper_name", output_path.name)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("논문", pname)
    c2.metric("페이지", report.get("total_pages", "?"))
    dig_status = "OK" if report.get("digitalized_pdf", {}).get("compiled") else "FAIL"
    kor_status = "OK" if report.get("korean_pdf", {}).get("compiled") else "FAIL"
    dig_err = list(output_path.glob("*_digitalized_error.log"))
    kor_err = list(output_path.glob("*_Korean_error.log"))
    if dig_err:
        dig_status = "FAIL"
    if kor_err:
        kor_status = "FAIL"
    c3.metric("디지털화 PDF", dig_status)
    c4.metric("한국어 PDF", kor_status)
    # Fallback: if pipeline log missed the "done" event, mark complete when report exists.
    if st.session_state.get("pipeline_running") and not st.session_state.get("pipeline_error"):
        st.session_state["pipeline_running"] = False
        st.session_state["pipeline_done"] = True
        st.session_state["pipeline_event_queue"] = None

st.divider()

# ── 탭 구성 ─────────────────────────────────────────────────────
tabs = st.tabs([
    "원본 이미지",
    "디지털화 PDF",
    "한국어 PDF",
    "용어집",
    "품질 보고서",
    "LaTeX 소스",
    "노트",
])

# ── 1. 원본 이미지 ──────────────────────────────────────────────
with tabs[0]:
    img_dir = output_path / "images"
    images = []
    if img_dir.is_dir():
        images = sorted(img_dir.glob("*.png")) + sorted(img_dir.glob("*.jpg"))
    if images:
        cols_per_row = st.slider("열 수", 1, 4, 3, key="img_cols")
        cols = st.columns(cols_per_row)
        for i, img in enumerate(images):
            with cols[i % cols_per_row]:
                st.image(str(img), caption=img.name, width="stretch")
    else:
        st.info("images/ 폴더에 이미지가 없습니다.")

# ── 2. 디지털화 PDF ─────────────────────────────────────────────
with tabs[1]:
    digitalized = sorted(output_path.glob("*_digitalized.pdf"))
    if digitalized:
        pdf_iframe(digitalized[0])
        download_btn(digitalized[0], "디지털화 PDF 다운로드")
    else:
        st.info("디지털화 PDF 파일이 없습니다.")

# ── 3. 한국어 PDF ───────────────────────────────────────────────
with tabs[2]:
    korean = sorted(output_path.glob("*_Korean.pdf"))
    if korean:
        pdf_iframe(korean[0])
        download_btn(korean[0], "한국어 PDF 다운로드")
    else:
        st.info("한국어 PDF 파일이 없습니다.")

# ── 4. 용어집 ───────────────────────────────────────────────────
with tabs[3]:
    init_db(get_db_path())
    paper_name = None
    if report:
        paper_name = report.get("paper_name")
    if not paper_name:
        paper_name = output_path.name
    glossary = fetch_terms_for_paper(get_db_path(), paper_name)
    if glossary:
        st.dataframe(glossary, width="stretch", height=500)
        st.caption(f"총 {len(glossary)}개 용어 (DB)")
    else:
        st.info("DB에 용어집이 없습니다.")

# ── 5. 품질 보고서 ──────────────────────────────────────────────
with tabs[4]:
    if report:
        st.json(report)
        download_btn(report_files[0], "품질 보고서 다운로드")
    else:
        st.info("품질 보고서 파일이 없습니다.")

# ── 6. LaTeX 소스 ───────────────────────────────────────────────
with tabs[5]:
    tex_files = sorted(output_path.glob("*.tex"))
    if tex_files:
        selected = st.selectbox("?? ??", tex_files, format_func=lambda p: p.name)
        default_content = selected.read_text(encoding="utf-8")
        state_key = f"latex_edit_{selected.name}"
        if state_key not in st.session_state:
            st.session_state[state_key] = default_content

        left, right = st.columns(2)
        with left:
            st.subheader("LaTeX ??")
            st.text_area(
                "?? ??",
                key=state_key,
                height=500,
                label_visibility="collapsed",
            )
            auto_compiler = "xelatex" if (
                "\\usepackage{kotex}" in st.session_state[state_key]
                or "\\setmainfont" in st.session_state[state_key]
            ) else "pdflatex"
            compiler = st.selectbox(
                "????",
                ["xelatex", "pdflatex"],
                index=0 if auto_compiler == "xelatex" else 1,
            )
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("???", key=f"compile_{selected.name}", type="primary", width="stretch"):
                    ok, pdf_path, error_log = compile_latex(
                        st.session_state[state_key],
                        str(output_path),
                        selected.stem,
                        compiler=compiler,
                    )
                    st.session_state[f"compile_ok_{selected.name}"] = ok
                    st.session_state[f"compile_err_{selected.name}"] = error_log
            with col_b:
                if st.button("?? ??", key=f"reset_{selected.name}", width="stretch"):
                    st.session_state[state_key] = default_content
                    st.rerun()

            compile_ok = st.session_state.get(f"compile_ok_{selected.name}")
            compile_err = st.session_state.get(f"compile_err_{selected.name}")
            if compile_ok is True:
                st.success("??? ??")
            elif compile_ok is False:
                st.error("??? ??")
                if compile_err:
                    st.code(compile_err, language="text")

        with right:
            st.subheader("PDF ????")
            pdf_path = output_path / f"{selected.stem}.pdf"
            if pdf_path.exists():
                pdf_iframe(pdf_path)
                download_btn(pdf_path, f"{pdf_path.name} ????")
            else:
                st.info("PDF? ????. ???? ?? ?????.")
    else:
        st.info("LaTeX ??? ????.")
with tabs[6]:
    note_files = sorted(output_path.glob("*_notes.txt"))
    if note_files:
        for nf in note_files:
            with st.expander(nf.name, expanded=True):
                st.text(nf.read_text(encoding="utf-8"))
                download_btn(nf, f"{nf.name} 다운로드")
    else:
        st.info("노트 파일이 없습니다.")
