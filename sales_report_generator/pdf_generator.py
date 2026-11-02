"""
Renders the Jinja2 template to HTML, then converts it to a PDF file with
xhtml2pdf. xhtml2pdf is pure Python (built on reportlab) so there are no
system-level dependencies to install (unlike WeasyPrint's Pango/Cairo).
"""
import os
import re
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
from xhtml2pdf import pisa

import config

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    return slug or "customer"


def render_customer_pdf(customer: dict, sales: list[dict], analytics: dict, period_label: str, output_dir: str) -> str:
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template("report.html")

    # Embed the bundled DejaVu Sans font so the Naira symbol (and any other
    # non-ASCII text) renders correctly regardless of what fonts happen to
    # be installed on the machine running this script.
    font_regular = os.path.join(TEMPLATE_DIR, "fonts", "DejaVuSans.ttf").replace(os.sep, "/")
    font_bold = os.path.join(TEMPLATE_DIR, "fonts", "DejaVuSans-Bold.ttf").replace(os.sep, "/")

    html = template.render(
        company_name=config.COMPANY_NAME,
        currency=config.CURRENCY_SYMBOL,
        customer=customer,
        sales=sales,
        analytics=analytics,
        period_label=period_label,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        font_regular=font_regular,
        font_bold=font_bold,
    )

    os.makedirs(output_dir, exist_ok=True)
    filename = f"sales_report_{_slugify(customer.get(config.CUSTOMER_NAME_COL, 'customer'))}_{datetime.now().strftime('%Y%m%d')}.pdf"
    out_path = os.path.join(output_dir, filename)

    with open(out_path, "wb") as f:
        result = pisa.CreatePDF(src=html, dest=f)

    if result.err:
        raise RuntimeError(f"Failed to render PDF for {customer.get(config.CUSTOMER_NAME_COL)}")

    return out_path
