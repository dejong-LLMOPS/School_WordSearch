# Dashboard Improvement Plan: Usability & Visual Design

This plan covers the Dash app in `map_app.py` and layouts in `dashboard_layouts.py`: **State Overview** (map + national summary + state panel + district table) and **District Detail** (side panel + full page).

---

## 1. Design system (foundation)

**Current state:** Inline styles everywhere; colors in `map_app.py` constants; no typography scale or spacing system; no custom CSS.

**Actions:**

- **Add `assets/dashboard.css`**  
  Dash auto-serves `assets/` from the app directory. Use one main stylesheet for layout, typography, and components so the UI is consistent and easier to change.

- **Define a small design token set** (in CSS or a Python dict used by layout helpers):
  - **Colors:** Keep existing semantic colors (school blue, district orange, total green, neutral gray) but add:
    - Surface: page background, card background, panel background
    - Border: subtle borders (e.g. `#e8e8e8`), stronger for focus
    - Text: primary, secondary, muted
  - **Typography:** Base font (e.g. system-ui or a single Google font), clear hierarchy:
    - Page title (H1), section title (H2/H3), body, small/caption
  - **Spacing:** 4/8/12/16/24/32 px scale for padding/margins/gaps.
  - **Radius & shadow:** Consistent border-radius (e.g. 8px cards, 6px buttons) and light box-shadows for cards/panels.

- **Refactor layout code** to use classes from `dashboard.css` instead of duplicating the same inline styles (e.g. card containers, section headings, summary panels). Keep minimal inline style only where dynamic (e.g. color from constants).

**Outcome:** Consistent look, easier tweaks, less duplicated style logic.

---

## 2. State Overview page

**Current state:** National summary cards, choropleth map, state summary panel (on click), district DataTable; right-side district detail panel. Works but feels dense and generic.

**Usability:**

- **Navigation / orientation**
  - Add a clear **app header** (e.g. “School Policy Term Dashboard”) and optional **breadcrumb** on State Overview: “State Overview” so users know where they are.
  - When a state is selected, show the **state name in the header or above the table** (e.g. “Districts in Florida”) so it’s obvious what the table shows without re-clicking the map.

- **Map**
  - Add a short **legend or subtitle** explaining what “share” means (e.g. “Share of national keyword hits”) and that clicking a state filters the list.
  - Improve **hover/click feedback**: ensure hover tooltip is readable; consider a subtle outline or highlight on the selected state so the selection is visible at a glance.
  - Consider **responsive height** (e.g. `min-height` + max) so the map doesn’t dominate on small screens.

- **National summary**
  - Keep the four metric cards but style them with the new card class (padding, radius, shadow) and consistent typography (number prominent, label muted).
  - On narrow screens, stack cards in a 2×2 or single column so they don’t squeeze.

- **State summary panel**
  - When no state is selected, use a friendly **empty state** (e.g. “Click a state on the map to see summary and districts”) instead of plain text.
  - When a state is selected, keep metric cards + keyword bar chart; align styling with national summary cards (same card class, spacing).

- **District table**
  - Keep sort/filter; ensure **row hover and selected state** are obvious (background + cursor).
  - Make the **“View” action clearer**: e.g. “View” link/button only on rows with hits, or a distinct “View details” so users know what to click.
  - Consider **sticky header** when scrolling long lists (Dash DataTable supports this via `fixed_rows={'headers': True}` if needed).
  - Optional: **row count** above the table (“Showing 42 districts”) so users know how many rows there are.

**Visual:**

- Apply the design system: page background, card style, section headings (H2/H3), consistent gaps between map, summary, and table.
- Give the map container a light border and radius so it reads as a clear “widget.”
- Use the same panel style for the state summary and the district detail side panel (background, padding, border).

**Outcome:** Clearer navigation, better affordances (what to click, what “share” means), and a more cohesive State Overview.

---

## 3. District detail (side panel + full page)

**Current state:** Detail shown in a right-side panel (from table click) or full page (from URL). Content: header, stat cards, terms list, AI summary (iframe or pre-wrap), links.

**Usability:**

- **Panel vs page**
  - **Side panel:** Add a visible **“Close”** control (you have one; ensure it’s prominent) and optionally **“Open full page”** link to `/dashboard/district/<id>` so users can get a dedicated view or share a link.
  - **Full page:** Keep “Back to State Overview” as primary action; optionally show **breadcrumb**: State Overview › &lt;State&gt; › &lt;District&gt; so users can jump back to state context.

- **Content order and scanability**
  - Keep: Header (name, state) → Term occurrence summary (cards) → Terms found → AI summary → Links.
  - Add a **short intro line** under the header if helpful (e.g. “Keyword hits and AI summary for this district”).
  - **Stat cards:** Use the same card component as State Overview (number large, label small, semantic colors).

- **Terms found**
  - If the list is long, consider **tags or chips** instead of a long comma list, or a compact list with max height + “Show more” so the section doesn’t dominate.

- **AI summary**
  - **Iframe:** Increase default height or make it **resizable** (e.g. min-height 400px, overflow auto), and ensure the iframe body uses the same font/size as the rest of the app so it doesn’t feel like a different app.
  - **Fallback (raw text):** Use the same card/panel style and good line-height so it’s readable.
  - If the summary is empty, show a clear **empty state** (“No AI summary for this district”) instead of a blank area.

- **Links**
  - Keep “Links to Pages with Terms” as a list; make links clearly clickable (color, underline on hover) and optionally **truncate long URLs** with a tooltip or “Copy” for long strings.
  - If there are many links, cap at 50 as now but add “Showing first 50 of N links” so users know there are more.

**Visual:**

- Use one **section style** for “Term Occurrence Summary,” “Terms Found,” “AI Summary,” “Links” (same heading size, spacing, optional left border or icon).
- Apply card style to the summary cards and to the AI summary container.
- Ensure the side panel has a clear separation (border or background) from the main content and doesn’t feel cramped (padding, scroll only when needed).

**Outcome:** District detail is easier to scan, AI summary and links are clearer, and navigation between panel and full page is obvious.

---

## 4. Global navigation and layout

**Current state:** Only `dcc.Location` and `page-content`; no shared chrome.

**Actions:**

- **Wrapper layout**
  - Wrap `page-content` in a **main wrapper** that always includes:
    - Optional **header** (title + breadcrumb or “State Overview” when on overview).
    - Optional **footer** (e.g. “School Policy Term Dashboard · Data from …”) for context.
  - Use the same wrapper (and CSS) for State Overview and District Detail so the app feels like one flow.

- **Breadcrumbs**
  - State Overview: “State Overview.”
  - District full page: “State Overview › &lt;State name&gt; › &lt;District name&gt;” (links for the first two segments).
  - Side panel: Reuse the same breadcrumb snippet so context is clear.

- **Responsive behavior**
  - **Desktop:** Keep current two-column idea (map + state panel; table below; district detail in side panel).
  - **Tablet/small desktop:** Consider stacking map above state panel; table full width; side panel below or as a modal/drawer when “View” is clicked.
  - **Mobile:** Single column; map first (smaller height); then summary cards (2×2 or stacked); then table (horizontal scroll if needed); district detail as full-page view or bottom sheet instead of side panel.
  - Use **CSS media queries** in `assets/dashboard.css` and flex/grid so the same structure adapts without duplicating logic.

**Outcome:** Consistent chrome, clear place-in-app, and better behavior on small screens.

---

## 5. Loading and empty states

**Current state:** No loading indicators; empty states are plain text.

**Actions:**

- **Loading**
  - For any callback that might take time (e.g. if you add future data reload), use `dcc.Loading` around the relevant `html.Div` and show a simple spinner or “Loading…” so the UI doesn’t look stuck.
  - Map and table are fed from stores, so initial load is fast; loading is more important if you add “Refresh” or heavy filters later.

- **Empty states**
  - **No state selected:** “Click a state on the map to see summary and districts” with a light background and optional icon.
  - **No districts in state:** “No districts in this state” with the same panel style.
  - **No district selected (side panel):** “Select a district to view details and AI summary” (already present; style consistently).
  - **District has no AI summary:** “No AI summary for this district” instead of blank.
  - **District has no links:** “No links with terms” or hide the section.

Use one **empty-state** class (centered text, muted color, padding) so all of these look consistent.

**Outcome:** Users always see a clear explanation when there’s no data or no selection.

---

## 6. Implementation order (suggested)

1. **Design system**
   - Add `assets/dashboard.css` with variables (or a small set of classes) for colors, typography, spacing, cards, panels.
   - Refactor one screen (e.g. State Overview) to use classes; then apply to District Detail.

2. **State Overview**
   - Add header/breadcrumb; improve map legend and empty state for state panel; unify card styling; clarify district table “View” and selection feedback.

3. **District detail**
   - Unify section and card styling; improve AI summary height/resize and empty state; clarify links; add “Open full page” and breadcrumb on full page.

4. **Global layout**
   - Introduce main wrapper with optional header/footer; add breadcrumbs to both overview and district full page; wire responsive rules in CSS.

5. **Polish**
   - Add `dcc.Loading` where useful; standardize all empty states with the same class; quick pass for contrast and touch targets (button/link size) on mobile.

---

## 7. Optional enhancements (later)

- **Accessibility:** Ensure focus states on links/buttons, sufficient color contrast, and optional `aria-label`s for map and table.
- **Map:** State name search or dropdown to jump to a state (in addition to click).
- **Table:** Export to CSV for the current state’s districts.
- **District detail:** “Previous / Next district” within the same state for quick scanning.
- **Theme:** If you ever want dark mode, centralizing colors in CSS (or a theme module) will make a single toggle easier.

---

## Files to touch

| Area              | Files |
|-------------------|--------|
| New assets        | `assets/dashboard.css` |
| Layout / UI       | `map_app.py` (layouts, callbacks, color usage) |
| Layout helpers    | `dashboard_layouts.py` (if you keep using it for district full page) |
| Data (unchanged)  | `dashboard_data.py` (no change required for this plan) |

This plan keeps the current behavior and data flow while making the dashboard more usable, consistent, and easier to maintain. Implementing the design system and State Overview first will give the biggest visible gain with a clear path for the rest.
