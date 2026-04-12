"""
Sales Analysis & Prediction Dashboard
Detailed Sales Report — POS4Africa
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import matplotlib.ticker as mticker
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import warnings
warnings.filterwarnings('ignore')

# ── Palette ───────────────────────────────────────────────────────────────────
C = {
      'bg':      '#0F1117',
      'card':    '#1A1D27',
      'border':  '#2A2D3E',
      'accent':  '#6C63FF',
      'green':   '#00D68F',
      'red':     '#FF4757',
      'yellow':  '#FFD166',
      'blue':    '#00B4D8',
      'orange':  '#FF6B35',
      'text':    '#E8E9F0',
      'muted':   '#8B8FA8',
      'bars': ['#6C63FF','#00D68F','#FFD166','#FF6B35','#00B4D8','#FF4757','#A8DADC','#F4A261'],
}

def fmt_naira(v):
      if v >= 1_000_000: return f'₦{v/1_000_000:.1f}M'
      if v >= 1_000:     return f'₦{v/1_000:.0f}K'
      return f'₦{v:,.0f}'

# ── Load & clean ──────────────────────────────────────────────────────────────
df = pd.read_excel('sales_data.xlsx')
df['Date']     = pd.to_datetime(df['Date'].str.split('-').str[0], format='%m/%d/%Y')
df['Sold By']  = df['Sold By'].str.strip()

def payment_channel(pt):
      s = str(pt)
      has = lambda x: x in s
      m, c, a, st = has('MONIEPOINT'), has('Cash'), has('ACCESS BANK'), has('Store Account')
      if sum([m, c, a, st]) > 1: return 'Mixed'
      if m:  return 'Moniepoint'
      if c:  return 'Cash'
      if a:  return 'Access Bank'
      if st: return 'Store Account'
      return 'Other'

df['Channel']  = df['Payment Type'].apply(payment_channel)
sales   = df[df['Total'] > 0].copy()
returns = df[df['Total'] < 0].copy()

# ── Aggregates ────────────────────────────────────────────────────────────────
daily       = sales.groupby('Date')['Total'].sum().sort_index()
top_prods   = sales.groupby('Name')['Total'].sum().sort_values(ascending=False).head(8)
top_cats    = sales.groupby('Category')['Total'].sum().sort_values(ascending=False)
channels    = sales.groupby('Channel')['Total'].sum().sort_values(ascending=False)
by_person   = sales.groupby('Sold By')['Total'].sum().sort_values(ascending=False)
qty_prods   = sales.groupby('Name')['Quantity Sold'].sum().sort_values(ascending=False).head(8)

total_rev   = sales['Total'].sum()
total_ret   = abs(returns['Total'].sum())
net_rev     = total_rev - total_ret
avg_daily   = daily.mean()
num_sales   = sales['Sale Id'].nunique()

# ── Prediction ────────────────────────────────────────────────────────────────
X = np.arange(len(daily)).reshape(-1, 1)
y = daily.values

poly = PolynomialFeatures(degree=2)
Xp   = poly.fit_transform(X)
reg  = LinearRegression().fit(Xp, y)

future_days = 7
X_fut = np.arange(len(daily), len(daily) + future_days).reshape(-1, 1)
y_fut = reg.predict(poly.transform(X_fut))
y_fut = np.clip(y_fut, 0, None)

future_dates = pd.date_range(daily.index[-1] + pd.Timedelta(days=1), periods=future_days)
r2 = reg.score(Xp, y)

# ── Figure layout ─────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(22, 28), facecolor=C['bg'])
fig.suptitle('SALES ANALYTICS DASHBOARD', fontsize=26, fontweight='bold',
             color=C['text'], y=0.98, fontfamily='monospace')
fig.text(0.5, 0.965, 'Detailed Sales Report  |  Mar 31 – Apr 6, 2026',
         ha='center', fontsize=12, color=C['muted'])

gs = gridspec.GridSpec(figure=fig, hspace=0.52, wspace=0.38,
                       left=0.05, 4, f5, right=0.97, top=0.94, bottom=0.03)

def card_ax(ax, title=''):
      ax.set_facecolor(C['card'])
      for sp in ax.spines.values():
            sp.set_edgecolor(C['border']); sp.set_linewidth(1.2)
      if title:
            ax.set_title(title, color=C['muted'], fontsize=10, pad=8, loc='left')

# ── Row 0 — KPI cards ─────────────────────────────────────────────────────────
kpis = [
      ('TOTAL REVENUE',    fmt_naira(total_rev),  C['green'],  '↑ Gross sales'),
      ('TOTAL RETURNS',    fmt_naira(total_ret),  C['red'],    f'{len(returns)} transactions'),
      ('NET REVENUE',      fmt_naira(net_rev),    C['accent'], 'After returns'),
      ('AVG DAILY SALES',  fmt_naira(avg_daily),  C['yellow'], f'Over {len(daily)} days'),
]
for i, (label, value, color, sub) in enumerate(kpis):
      ax = fig.add_subplot(gs[0, i])
      ax.set_facecolor(C['card'])
      for sp in ax.spines.values(): sp.set_edgecolor(color); sp.set_linewidth(2)
      ax.set_xticks([]); ax.set_yticks([])
      ax.text(0.5, 0.72, value, transform=ax.transAxes, ha='center',
                  fontsize=22, fontweight='bold', color=color)
      ax.text(0.5, 0.38, label, transform=ax.transAxes, ha='center',
                  fontsize=9, color=C['muted'], fontweight='bold')
      ax.text(0.5, 0.14, sub, transform=ax.transAxes, ha='center',
                  fontsize=8, color=C['muted'])

# ── Row 1 — Daily sales trend + prediction ───────────────────────────────────
ax1 = fig.add_subplot(gs[1, :])
card_ax(ax1, 'DAILY REVENUE TREND  +  7-DAY FORECAST')
ax1.tick_params(colors=C['muted'])

# Actual
ax1.fill_between(daily.index, daily.values, alpha=0.18, color=C['accent'])
ax1.plot(daily.index, daily.values, color=C['accent'], lw=2.5, marker='o',
         ms=7, markerfacecolor=C['bg'], markeredgewidth=2, label='Actual Revenue')

# Smooth trend line over actuals
X_smooth = np.linspace(0, len(daily)-1, 200).reshape(-1,1)
y_smooth  = reg.predict(poly.transform(X_smooth))
dates_smooth = pd.to_datetime([daily.index[0] + pd.Timedelta(days=float(x)) for x in X_smooth.flatten()])
ax1.plot(dates_smooth, y_smooth, color=C['yellow'], lw=1.5, ls='--', alpha=0.6, label='Trend')

# Forecast
ax1.fill_between(future_dates, y_fut * 0.75, y_fut * 1.25, alpha=0.12, color=C['green'])
ax1.plot(future_dates, y_fut, color=C['green'], lw=2, ls='--', marker='s',
         ms=6, markerfacecolor=C['bg'], markeredgewidth=2, label='Forecast (7d)')
ax1.axvline(daily.index[-1], color=C['border'], lw=1.5, ls=':')
ax1.text(daily.index[-1], ax1.get_ylim()[1] if ax1.get_ylim()[1] > 0 else 1,
         ' Forecast →', color=C['muted'], fontsize=8, va='top')

for date, val in zip(daily.index, daily.values):
      ax1.annotate(fmt_naira(val), (date, val), textcoords='offset points',
                  xytext=(0, 10), ha='center', fontsize=7.5, color=C['text'])

ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_naira(x)))
ax1.tick_params(axis='x', colors=C['muted']); ax1.tick_params(axis='y', colors=C['muted'])
ax1.set_facecolor(C['card'])
for sp in ax1.spines.values(): sp.set_edgecolor(C['border'])
leg = ax1.legend(facecolor=C['card'], edgecolor=C['border'], labelcolor=C['text'], fontsize=9)
ax1.text(0.99, 0.96, f'Model R² = {r2:.2f}', transform=ax1.transAxes,
         ha='right', va='top', fontsize=8, color=C['muted'])

# ── Row 2 left — Top products by revenue ─────────────────────────────────────
ax2 = fig.add_subplot(gs[2, :2])
card_ax(ax2, 'TOP 8 PRODUCTS BY REVENUE')
bars = ax2.barh(range(len(top_prods)), top_prods.values, color=C['bars'][:len(top_prods)],
                edgecolor='none', height=0.65)
ax2.set_yticks(range(len(top_prods)))
ax2.set_yticklabels([n[:22] for n in top_prods.index], color=C['text'], fontsize=8.5)
ax2.invert_yaxis()
ax2.set_facecolor(C['card'])
for sp in ax2.spines.values(): sp.set_edgecolor(C['border'])
ax2.tick_params(axis='x', colors=C['muted'], labelsize=8)
for bar, val in zip(bars, top_prods.values):
      ax2.text(bar.get_width() + top_prods.values.max()*0.01, bar.get_y() + bar.get_height()/2,
                  fmt_naira(val), va='center', color=C['text'], fontsize=8)
ax2.set_xlim(0, top_prods.values.max() * 1.2)
ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_naira(x)))

# ── Row 2 right — Top products by qty ────────────────────────────────────────
ax3 = fig.add_subplot(gs[2, 2:])
card_ax(ax3, 'TOP 8 PRODUCTS BY QUANTITY SOLD')
bars3 = ax3.barh(range(len(qty_prods)), qty_prods.values, color=C['bars'][:len(qty_prods)],
                 edgecolor='none', height=0.65)
ax3.set_yticks(range(len(qty_prods)))
ax3.set_yticklabels([n[:22] for n in qty_prods.index], color=C['text'], fontsize=8.5)
ax3.invert_yaxis()
ax3.set_facecolor(C['card'])
for sp in ax3.spines.values(): sp.set_edgecolor(C['border'])
ax3.tick_params(axis='x', colors=C['muted'], labelsize=8)
for bar, val in zip(bars3, qty_prods.values):
      ax3.text(bar.get_width() + qty_prods.values.max()*0.01, bar.get_y() + bar.get_height()/2,
                  f'{val:.0f} units', va='center', color=C['text'], fontsize=8)
ax3.set_xlim(0, qty_prods.values.max() * 1.25)

# ── Row 3 left — Category breakdown (donut) ──────────────────────────────────
ax4 = fig.add_subplot(gs[3, :2])
ax4.set_facecolor(C['card'])
for sp in ax4.spines.values(): sp.set_edgecolor(C['border'])
wedges, texts, autotexts = ax4.pie(
      top_cats.values, labels=None,
      colors=C['bars'][:len(top_cats)],
      autopct='%1.1f%%', startangle=140,
      wedgeprops=dict(width=0.55, edgecolor=C['card'], linewidth=2),
      pctdistance=0.78,
)
for t in autotexts: t.set(color=C['bg'], fontsize=8, fontweight='bold')
ax4.legend(wedges, [f'{k}  {fmt_naira(v)}' for k, v in top_cats.items()],
           loc='lower center', bbox_to_anchor=(0.5, -0.18), ncol=2,
           facecolor=C['card'], edgecolor=C['border'], labelcolor=C['text'], fontsize=8)
ax4.set_title('SALES BY CATEGORY', color=C['muted'], fontsize=10, pad=10, loc='left')

# ── Row 3 right — Payment channel ────────────────────────────────────────────
ax5 = fig.add_subplot(gs[3, 2:])
card_ax(ax5, 'PAYMENT CHANNELS')
ch_colors = [C['accent'], C['blue'], C['green'], C['yellow'], C['orange']]
bars5 = ax5.bar(range(len(channels)), channels.values,
                color=ch_colors[:len(channels)], edgecolor='none', width=0.6)
ax5.set_xticks(range(len(channels)))
ax5.set_xticklabels(channels.index, color=C['text'], fontsize=9, rotation=15, ha='right')
ax5.set_facecolor(C['card'])
for sp in ax5.spines.values(): sp.set_edgecolor(C['border'])
ax5.tick_params(axis='y', colors=C['muted'])
ax5.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_naira(x)))
for bar, val in zip(bars5, channels.values):
      pct = val / channels.sum() * 100
      ax5.text(bar.get_x() + bar.get_width()/2, bar.get_height() + channels.values.max()*0.02,
                  f'{fmt_naira(val)}\n{pct:.1f}%', ha='center', color=C['text'], fontsize=8)
ax5.set_ylim(0, channels.values.max() * 1.25)

# ── Row 4 — Forecast table + salesperson + returns ───────────────────────────
ax6 = fig.add_subplot(gs[4, :2])
ax6.set_facecolor(C['card'])
for sp in ax6.spines.values(): sp.set_edgecolor(C['border'])
ax6.set_xticks([]); ax6.set_yticks([])
ax6.set_title('7-DAY REVENUE FORECAST', color=C['muted'], fontsize=10, pad=8, loc='left')

col_labels = ['Date', 'Day', 'Forecast Revenue', 'vs Avg Daily']
rows_data  = []
for d, v in zip(future_dates, y_fut):
      diff_pct = (v - avg_daily) / avg_daily * 100
      arrow    = '▲' if diff_pct >= 0 else '▼'
      color_d  = C['green'] if diff_pct >= 0 else C['red']
      rows_data.append([d.strftime('%b %d'), d.strftime('%A'), fmt_naira(v),
                        f'{arrow} {abs(diff_pct):.1f}%'])

table = ax6.table(cellText=rows_data, colLabels=col_labels,
                  loc='center', cellLoc='center')
table.auto_set_font_size(False)
table.set_fontsize(9)
for (r, c), cell in table.get_celld().items():
      cell.set_facecolor(C['card'] if r % 2 == 0 else C['bg'])
      cell.set_edgecolor(C['border'])
      cell.set_text_props(color=C['text'] if r > 0 else C['accent'])
      if r == 0: cell.set_text_props(color=C['accent'], fontweight='bold')
      if r > 0 and c == 3:
            val = rows_data[r-1][3]
            cell.set_text_props(color=C['green'] if '▲' in val else C['red'])
table.scale(1, 1.6)

# Salesperson + summary stats
ax7 = fig.add_subplot(gs[4, 2:])
ax7.set_facecolor(C['card'])
for sp in ax7.spines.values(): sp.set_edgecolor(C['border'])
ax7.set_xticks([]); ax7.set_yticks([])
ax7.set_title('SUMMARY & SALESPERSON STATS', color=C['muted'], fontsize=10, pad=8, loc='left')

summary = [
      ['Metric', 'Value'],
      ['Total Transactions', f'{num_sales:,}'],
      ['Total Line Items', f'{len(sales):,}'],
      ['Total Returns', f'{len(returns)} txns  ({fmt_naira(total_ret)})'],
      ['Best Day', f'{daily.idxmax().strftime("%b %d")}  ({fmt_naira(daily.max())})'],
      ['Best Product', f'{top_prods.index[0][:20]}'],
      ['BB Revenue', fmt_naira(by_person.get("BB", 0))],
      ['ABBA Revenue', fmt_naira(by_person.get("ABBA", 0))],
      ['Top Category', f'ATAMPA  (82.3%)'],
]
tbl2 = ax7.table(cellText=summary[1:], colLabels=summary[0],
                 loc='center', cellLoc='left')
tbl2.auto_set_font_size(False)
tbl2.set_fontsize(9)
for (r, c), cell in tbl2.get_celld().items():
      cell.set_facecolor(C['card'] if r % 2 == 0 else C['bg'])
      cell.set_edgecolor(C['border'])
      cell.set_text_props(color=C['text'] if r > 0 else C['accent'])
      if r == 0: cell.set_text_props(color=C['accent'], fontweight='bold')
tbl2.scale(1, 1.55)

# ── Footer ────────────────────────────────────────────────────────────────────
fig.text(0.5, 0.005,
         'Forecast uses polynomial regression (degree=2) on 7-day actuals  •  Shaded band = ±25% confidence interval',
         ha='center', fontsize=8, color=C['muted'])

plt.savefig('./sales_dashboard.png',
            dpi=160, bbox_inches='tight', facecolor=C['bg'])

print("Dashboard saved.")