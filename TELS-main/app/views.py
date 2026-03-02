from django.views.decorators.cache import never_cache

import json
import csv
import io
import re
from collections import defaultdict

from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import render, get_object_or_404, redirect

from django.contrib import messages
from django.contrib.auth import authenticate, login
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required, user_passes_test

from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme

from .models import Customer, TEPCode, Material, MaterialList, Forecast
from .forms import EmployeeCreateForm

from django.contrib.auth import logout
from django.shortcuts import redirect
from django.urls import reverse


def is_admin(user):
    return user.is_authenticated and user.is_superuser


def can_edit(user):
    return user.is_authenticated and user.is_staff


def home(request):
    return HttpResponse("Welcome to the Home Page!")


def login_view(request):
    error = ""

    if request.method == "POST":
        employee_id = (request.POST.get("employee_id") or "").strip()
        password = request.POST.get("password") or ""

        user = authenticate(request, username=employee_id, password=password)

        if user is not None and user.is_active:
            login(request, user)

            if user.is_superuser:
                return redirect("app:admin_dashboard")

            if user.is_staff:
                return redirect("app:customer_list")

            return redirect("app:customer_list")
        else:
            error = "Invalid Employee ID or password"

    return render(request, "login.html", {"error": error})


def _normalize_space(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def _unique_partname_for_customer(customer, base_name, part_code):
    base_name = _normalize_space(base_name)
    part_code = _normalize_space(part_code)

    parts = customer.parts or []

    for p in parts:
        if isinstance(p, dict) and _normalize_space(p.get("Partcode")) == part_code:
            existing = _normalize_space(p.get("Partname"))
            return existing or base_name

    existing_names = set()
    for p in parts:
        if isinstance(p, dict):
            n = _normalize_space(p.get("Partname"))
            if n:
                existing_names.add(n.lower())

    if base_name.lower() not in existing_names:
        return base_name

    i = 1
    while True:
        candidate = f"{base_name} {i}"
        if candidate.lower() not in existing_names:
            return candidate
        i += 1


def _ensure_customer_part_entry(customer, part_code, part_name):
    part_code = _normalize_space(part_code)
    part_name = _normalize_space(part_name) or part_code

    parts = customer.parts or []

    for p in parts:
        if isinstance(p, dict) and _normalize_space(p.get("Partcode")) == part_code:
            used = _normalize_space(p.get("Partname")) or part_name
            return False, used

    unique_name = _unique_partname_for_customer(customer, part_name, part_code)
    parts.append({"Partcode": part_code, "Partname": unique_name})
    customer.parts = parts
    customer.save(update_fields=["parts"])
    return True, unique_name


def _allocate_material_name(tep, base_name: str, exclude_partcode: str = "") -> str:
    base = (base_name or "").strip() or "UNKNOWN"
    exclude_partcode = (exclude_partcode or "").strip()

    qs = Material.objects.filter(
        tep_code=tep,
        mat_partname__iregex=rf"^{re.escape(base)}( \d+)?$"
    )
    if exclude_partcode:
        qs = qs.exclude(mat_partcode=exclude_partcode)

    existing_names = list(qs.values_list("mat_partname", flat=True))
    if not existing_names:
        return base

    numbers = []
    for n in existing_names:
        m = re.match(rf"^{re.escape(base)}(?: (\d+))?$", (n or "").strip(), flags=re.IGNORECASE)
        if m and m.group(1):
            numbers.append(int(m.group(1)))

    if not numbers:
        existing_base = Material.objects.filter(tep_code=tep, mat_partname__iexact=base)
        if exclude_partcode:
            existing_base = existing_base.exclude(mat_partcode=exclude_partcode)

        first = existing_base.order_by("id").first()
        if first:
            first.mat_partname = f"{base} 1"
            first.save(update_fields=["mat_partname"])

        return f"{base} 2"

    return f"{base} {max(numbers) + 1}"


def build_customer_table(q: str):
    qs = (
        Customer.objects
        .prefetch_related("tep_codes__materials")
        .order_by("customer_name")
    )

    if q:
        qs = qs.filter(
            Q(customer_name__icontains=q)
            | Q(tep_codes__tep_code__icontains=q)
            | Q(tep_codes__part_code__icontains=q)
            | Q(tep_codes__materials__mat_partcode__icontains=q)
            | Q(tep_codes__materials__mat_partname__icontains=q)
            | Q(tep_codes__materials__mat_maker__icontains=q)
        ).distinct()

    grouped = defaultdict(lambda: {
        "parts_by_code": {},
        "teps_by_part": defaultdict(list),
    })

    for cust in qs:
        name = cust.customer_name

        for p in cust.parts or []:
            if not isinstance(p, dict):
                continue
            pc = (p.get("Partcode") or "").strip()
            pn = (p.get("Partname") or "").strip()
            if pc and pc not in grouped[name]["parts_by_code"]:
                grouped[name]["parts_by_code"][pc] = pn

        for tep in cust.tep_codes.all():
            grouped[name]["teps_by_part"][tep.part_code].append(tep)

    customers = []

    for name, g in grouped.items():
        parts_by_code = g["parts_by_code"]
        teps_by_part = g["teps_by_part"]

        part_code_options = sorted(parts_by_code.keys())
        part_code_map = {}

        for pc in part_code_options:
            tep_objs = sorted(teps_by_part.get(pc, []), key=lambda t: t.tep_code)

            teps = [
                {
                    "tep_id": t.id,
                    "tep_code": t.tep_code,
                    "materials_count": t.materials.count(),
                }
                for t in tep_objs
            ]

            default_tep = teps[0] if teps else None

            part_code_map[pc] = {
                "part_name": parts_by_code.get(pc, ""),
                "teps": teps,
                "default_tep_id": default_tep["tep_id"] if default_tep else None,
                "default_tep_code": default_tep["tep_code"] if default_tep else "",
                "default_materials_count": default_tep["materials_count"] if default_tep else 0,
            }

        default_pc = part_code_options[0] if part_code_options else ""

        customers.append({
            "customer_name": name,
            "part_code_options": part_code_options,
            "default_part_code": default_pc,

            "default_tep_options": part_code_map.get(default_pc, {}).get("teps", []),
            "default_tep_id": part_code_map.get(default_pc, {}).get("default_tep_id"),
            "default_tep_code": part_code_map.get(default_pc, {}).get("default_tep_code", ""),
            "default_materials_count": part_code_map.get(default_pc, {}).get("default_materials_count", 0),

            "part_code_map_json": json.dumps(part_code_map, ensure_ascii=False),
        })

    return customers


def _build_forecast_summary(fsq: str = "", fsq_customer: str = ""):
    """
    Build data for the Forecast Summary tab.

    This version groups rows by (customer, part_number) so that
    previous-year quantities and current-year quantities for the same
    part appear on a single row. It also exposes all 12 months
    (JAN–DEC) for both PREVIOUS FORECAST and FORECAST sections.

    Returned keys:
      - fs_rows: list of dicts with
          { customer, part_number, part_name, unit_price, prev, fore }
        where prev/fore are { "JAN": qty, ... }.
      - fs_prev_months, fs_fore_months: ordered month labels
        (always ["JAN", ..., "DEC"]).
      - fs_total_prev_qty / fs_total_prev_amt: totals per month.
      - fs_total_fore_qty / fs_total_fore_amt: totals per month.
      - fs_prev_year, fs_fore_year: year labels for headers.
      - fs_customers: distinct customer names for filter dropdown.
    """
    from datetime import date
    import calendar

    MONTHS_ORDER = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    SHORT_MONTHS = {
        1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
        7: "JUL", 8: "AUG", 9: "SEPT", 10: "OCT", 11: "NOV", 12: "DEC",
    }

    today = date.today()
    current_year = today.year
    prev_year = current_year - 1

    # ── fetch forecasts ──────────────────────────────────────────────────────
    qs = (
        Forecast.objects
        .select_related("customer")
        .order_by("customer__customer_name", "part_number")
    )

    if fsq:
        qs = qs.filter(
            Q(part_number__icontains=fsq) | Q(part_name__icontains=fsq)
        )
    if fsq_customer:
        qs = qs.filter(customer__customer_name=fsq_customer)

    # ── collect all month keys so we can build ordered column lists ──────────
    prev_month_keys = set()   # (year, month_int, label)
    fore_month_keys = set()

    def _parse_date_str(date_str):
        """Parse 'Month-YYYY' → (year:int, month_int:int, label:str) or None."""
        date_str = (date_str or "").strip()
        if not date_str:
            return None
        parts = date_str.split("-")
        if len(parts) < 2:
            return None
        month_name = parts[0].strip().lower()
        try:
            year = int(parts[-1].strip())
        except ValueError:
            return None
        month_int = MONTHS_ORDER.get(month_name)
        if not month_int:
            return None
        label = SHORT_MONTHS[month_int]
        return year, month_int, label

    # ── aggregate by (customer, part_number) so prev/fore share one row ─────
    rows_by_key = {}

    for forecast in qs:
        monthly = forecast.monthly_forecasts or []

        first_entry = next(
            (m for m in monthly if isinstance(m, dict)),
            {}
        )

        try:
            unit_price = float(first_entry.get("unit_price", 0)) if isinstance(first_entry, dict) else 0.0
        except (TypeError, ValueError):
            unit_price = 0.0

        customer_name = forecast.customer.customer_name if forecast.customer else "—"
        key = (customer_name, forecast.part_number)

        row = rows_by_key.get(key)
        if not row:
            row = {
                "customer": customer_name,
                "part_number": forecast.part_number,
                "part_name": forecast.part_name,
                "unit_price": unit_price,
                "prev": {},
                "fore": {},
            }
            rows_by_key[key] = row
        else:
            # keep latest non-zero unit price
            if unit_price:
                row["unit_price"] = unit_price

        prev_data = row["prev"]
        fore_data = row["fore"]

        for entry in monthly:
            if not isinstance(entry, dict):
                continue
            parsed = _parse_date_str(entry.get("date", ""))
            if not parsed:
                continue
            yr, mo, label = parsed
            try:
                qty = float(entry.get("quantity", 0) or 0)
            except (TypeError, ValueError):
                qty = 0.0

            # classify: year before current → previous; current/future → forecast
            if yr < current_year:
                prev_data[label] = prev_data.get(label, 0.0) + qty
                prev_month_keys.add((yr, mo, label))
            else:
                fore_data[label] = fore_data.get(label, 0.0) + qty
                fore_month_keys.add((yr, mo, label))

    fs_rows = list(rows_by_key.values())

    # ── build ordered month label lists (always full JAN–DEC) ───────────────
    all_month_labels = [SHORT_MONTHS[i] for i in range(1, 13)]

    # We still evaluate used labels to preserve behaviour if needed later,
    # but the context always exposes the full 12 months.
    _ = [lbl for (_, _, lbl) in sorted(prev_month_keys)]
    _ = [lbl for (_, _, lbl) in sorted(fore_month_keys)]

    fs_prev_months = all_month_labels
    fs_fore_months = all_month_labels

    # ── compute totals ───────────────────────────────────────────────────────
    fs_total_prev_qty = defaultdict(float)
    fs_total_fore_qty = defaultdict(float)
    fs_total_prev_amt = defaultdict(float)
    fs_total_fore_amt = defaultdict(float)

    for row in fs_rows:
        up = row["unit_price"]
        for lbl, qty in row["prev"].items():
            fs_total_prev_qty[lbl] += qty
            fs_total_prev_amt[lbl] += qty * up
        for lbl, qty in row["fore"].items():
            fs_total_fore_qty[lbl] += qty
            fs_total_fore_amt[lbl] += qty * up

    # ── customer list for filter dropdown ───────────────────────────────────
    fs_customers = list(
        Forecast.objects.select_related("customer")
        .values_list("customer__customer_name", flat=True)
        .distinct()
        .order_by("customer__customer_name")
    )

    return {
        "fs_rows":            fs_rows,
        "fs_prev_months":     fs_prev_months,
        "fs_fore_months":     fs_fore_months,
        "fs_total_prev_qty":  dict(fs_total_prev_qty),
        "fs_total_fore_qty":  dict(fs_total_fore_qty),
        "fs_total_prev_amt":  dict(fs_total_prev_amt),
        "fs_total_fore_amt":  dict(fs_total_fore_amt),
        "fs_prev_year":       prev_year,
        "fs_fore_year":       current_year,
        "fs_customers":       fs_customers,
    }


def _build_actual_summary(adq: str = "", ad_customer: str = ""):
    """
    Build data for the Actual Delivered tab.

    Uses the same row structure as the Forecast Summary tab but reads
    \"actual_quantity\" from each monthly_forecasts entry.
    """
    from datetime import date

    MONTHS_ORDER = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    SHORT_MONTHS = {
        1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
        7: "JUL", 8: "AUG", 9: "SEPT", 10: "OCT", 11: "NOV", 12: "DEC",
    }

    def _parse_date_str(date_str):
        date_str = (date_str or "").strip()
        if not date_str:
            return None
        parts = date_str.split("-")
        if len(parts) < 2:
            return None
        month_name = parts[0].strip().lower()
        try:
            year = int(parts[-1].strip())
        except ValueError:
            return None
        month_int = MONTHS_ORDER.get(month_name)
        if not month_int:
            return None
        label = SHORT_MONTHS[month_int]
        return year, month_int, label

    today = date.today()
    current_year = today.year

    qs = (
        Forecast.objects
        .select_related("customer")
        .order_by("customer__customer_name", "part_number")
    )

    if adq:
        qs = qs.filter(
            Q(part_number__icontains=adq) | Q(part_name__icontains=adq)
        )
    if ad_customer:
        qs = qs.filter(customer__customer_name=ad_customer)

    rows_by_key = {}
    months_seen = set()
    years_seen = set()

    for forecast in qs:
        monthly = forecast.monthly_forecasts or []

        first_entry = next(
            (m for m in monthly if isinstance(m, dict)),
            {}
        )
        try:
            unit_price = float(first_entry.get("unit_price", 0) or 0) if isinstance(first_entry, dict) else 0.0
        except (TypeError, ValueError):
            unit_price = 0.0

        customer_name = forecast.customer.customer_name if forecast.customer else "—"
        key = (customer_name, forecast.part_number)

        row = rows_by_key.get(key)
        if not row:
            row = {
                "customer": customer_name,
                "part_number": forecast.part_number,
                "part_name": forecast.part_name,
                "unit_price": unit_price,
                "months": {},
            }
            rows_by_key[key] = row
        else:
            if unit_price:
                row["unit_price"] = unit_price

        months_map = row["months"]

        for entry in monthly:
            if not isinstance(entry, dict):
                continue
            if "actual_quantity" not in entry:
                continue

            parsed = _parse_date_str(entry.get("date", ""))
            if not parsed:
                continue
            yr, mo, label = parsed
            years_seen.add(yr)
            months_seen.add(label)

            try:
                qty = float(entry.get("actual_quantity", 0) or 0)
            except (TypeError, ValueError):
                qty = 0.0

            months_map[label] = months_map.get(label, 0.0) + qty

    ad_rows = list(rows_by_key.values())

    # Always expose full JAN–DEC for consistency
    ad_months = [SHORT_MONTHS[i] for i in range(1, 13)]

    from collections import defaultdict as _dd

    ad_total_qty = _dd(float)
    ad_total_amt = _dd(float)

    for row in ad_rows:
        up = row["unit_price"]
        for lbl, qty in row["months"].items():
            ad_total_qty[lbl] += qty
            ad_total_amt[lbl] += qty * up

    # Year label: prefer the main year we saw, otherwise current year
    ad_year = sorted(years_seen)[0] if years_seen else current_year

    ad_customers = list(
        Forecast.objects.filter(
            monthly_forecasts__0__actual_quantity__isnull=False
        )
        .select_related("customer")
        .values_list("customer__customer_name", flat=True)
        .distinct()
        .order_by("customer__customer_name")
    )

    return {
        "ad_rows": ad_rows,
        "ad_months": ad_months,
        "ad_total_qty": dict(ad_total_qty),
        "ad_total_amt": dict(ad_total_amt),
        "ad_year": ad_year,
        "ad_customers": ad_customers,
    }

@never_cache
@login_required
@user_passes_test(is_admin)
def admin_dashboard(request):
    tab = (request.GET.get("tab") or "customers").strip().lower()

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "add_customer_full":
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_code = _normalize_space(request.POST.get("part_code"))
            part_name = _normalize_space(request.POST.get("part_name"))
            tep_code = _normalize_space(request.POST.get("tep_code"))

            mat_partcode = _normalize_space(request.POST.get("mat_partcode"))
            dim_qty_raw = (request.POST.get("dim_qty") or "").strip()
            loss_raw = (request.POST.get("loss_percent") or "").strip()

            if not customer_name:
                messages.error(request, "Customer Name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            if not part_code:
                messages.error(request, "Partcode is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            if not part_name:
                messages.error(request, "Partname is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            if not tep_code:
                messages.error(request, "TEP Code is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            if not mat_partcode:
                messages.error(request, "Material Partcode is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            if not dim_qty_raw:
                messages.error(request, "Dim Qty is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            try:
                dim_qty = float(dim_qty_raw)
            except Exception:
                messages.error(request, "Dim Qty must be a number.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            loss_percent = 10.0
            if loss_raw != "":
                try:
                    loss_percent = float(loss_raw)
                except Exception:
                    messages.error(request, "Loss % must be a number.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            master = MaterialList.objects.filter(mat_partcode=mat_partcode).first()
            if not master:
                messages.error(request, f"mat_partcode not found in master list: {mat_partcode}")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            total = round(float(dim_qty) * (1 + (float(loss_percent) / 100.0)), 4)

            try:
                with transaction.atomic():
                    customer, _ = Customer.objects.get_or_create(customer_name=customer_name)

                    _ensure_customer_part_entry(customer, part_code, part_name)

                    tep, _ = TEPCode.objects.get_or_create(
                        customer=customer,
                        part_code=part_code,
                        tep_code=tep_code,
                    )

                    final_name = _allocate_material_name(
                        tep=tep,
                        base_name=master.mat_partname,
                        exclude_partcode=mat_partcode
                    )

                    material, created = Material.objects.get_or_create(
                        tep_code=tep,
                        mat_partcode=mat_partcode,
                        defaults={
                            "mat_partname": final_name,
                            "mat_maker": master.mat_maker,
                            "unit": master.unit,
                            "dim_qty": dim_qty,
                            "loss_percent": loss_percent,
                            "total": total,
                        }
                    )

                    if not created:
                        messages.error(request, f"Material already exists for TEP {tep_code} + {mat_partcode}.")
                        return redirect(reverse("app:admin_dashboard") + "?tab=customers")

                messages.success(
                    request,
                    f"Saved: {customer_name} | {part_code} | {tep_code} | {mat_partcode}"
                )
            except Exception as e:
                messages.error(request, f"Failed to save full customer record: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=customers")

        if action == "add_material":
            mat_partcode = (request.POST.get("mat_partcode") or "").strip()
            mat_partname = (request.POST.get("mat_partname") or "").strip()
            mat_maker = (request.POST.get("mat_maker") or "").strip()
            unit = (request.POST.get("unit") or "").strip().lower()

            allowed_units = {"pc", "pcs", "m", "g", "kg"}
            if unit not in allowed_units:
                unit = "pc"

            if not mat_partcode:
                messages.error(request, "Part Code is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=materials")

            try:
                obj, created = MaterialList.objects.get_or_create(
                    mat_partcode=mat_partcode,
                    defaults={
                        "mat_partname": mat_partname or mat_partcode,
                        "mat_maker": mat_maker or "Unknown",
                        "unit": unit,
                    }
                )

                if created:
                    messages.success(request, f"Added material: {mat_partcode}")
                else:
                    changed = False
                    if mat_partname and obj.mat_partname != mat_partname:
                        obj.mat_partname = mat_partname
                        changed = True
                    if mat_maker and obj.mat_maker != mat_maker:
                        obj.mat_maker = mat_maker
                        changed = True
                    if unit and obj.unit != unit:
                        obj.unit = unit
                        changed = True

                    if changed:
                        obj.save()
                        messages.success(request, f"Updated material: {mat_partcode}")
                    else:
                        messages.info(request, f"No changes for: {mat_partcode}")

            except Exception as e:
                messages.error(request, f"Failed to save material: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=materials")

        if action == "update_material":
            mat_id = (request.POST.get("mat_id") or "").strip()
            mat_partcode = (request.POST.get("mat_partcode") or "").strip()
            mat_partname = (request.POST.get("mat_partname") or "").strip()
            mat_maker = (request.POST.get("mat_maker") or "").strip()
            unit = (request.POST.get("unit") or "").strip().lower()

            allowed_units = {"pc", "pcs", "m", "g", "kg"}
            if unit not in allowed_units:
                unit = "pc"

            if not mat_id:
                messages.error(request, "Missing material ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=materials")

            try:
                obj = MaterialList.objects.get(id=mat_id)

                if not mat_partcode:
                    messages.error(request, "Part Code is required.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=materials")

                if mat_partcode != obj.mat_partcode:
                    if MaterialList.objects.filter(mat_partcode=mat_partcode).exclude(id=obj.id).exists():
                        messages.error(request, f"Part Code already exists: {mat_partcode}")
                        return redirect(reverse("app:admin_dashboard") + "?tab=materials")

                obj.mat_partcode = mat_partcode
                obj.mat_partname = mat_partname or mat_partcode
                obj.mat_maker = mat_maker or "Unknown"
                obj.unit = unit
                obj.save()

                messages.success(request, f"Saved changes: {obj.mat_partcode}")

            except MaterialList.DoesNotExist:
                messages.error(request, "Material not found.")
            except Exception as e:
                messages.error(request, f"Failed to update: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=materials")

        if action == "add_forecast":
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_name = _normalize_space(request.POST.get("part_name"))
            part_number = _normalize_space(request.POST.get("part_number"))
            month = request.POST.get("month")
            year = request.POST.get("year")
            unit_price = request.POST.get("unit_price")
            quantity = request.POST.get("quantity")

            if not customer_name:
                messages.error(request, "Customer name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_name:
                messages.error(request, "Part name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_number:
                messages.error(request, "Part number is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not month or not year:
                messages.error(request, "Month and year are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not unit_price:
                messages.error(request, "Unit price is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not quantity:
                messages.error(request, "Quantity is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            try:
                unit_price = float(unit_price)
                quantity = float(quantity)
            except ValueError:
                messages.error(request, "Unit price and quantity must be valid numbers.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            date_str = f"{month}-{year}"

            customer, created = Customer.objects.get_or_create(
                customer_name=customer_name,
                defaults={"parts": []}
            )

            existing_forecast = Forecast.objects.filter(
                customer=customer,
                part_number=part_number
            ).first()

            # If a forecast for this part already exists for the customer,
            # allow adding a new month as long as the (month, year) combo
            # does not already exist in its monthly_forecasts.
            if existing_forecast:
                monthly = existing_forecast.monthly_forecasts or []

                # Check for duplicate month/year
                duplicate = any(
                    isinstance(m, dict) and str(m.get("date", "")).strip().lower() == date_str.lower()
                    for m in monthly
                )

                if duplicate:
                    messages.error(
                        request,
                        f"Forecast for part '{part_number}' already exists for {date_str} for this customer."
                    )
                    return redirect(
                        reverse("app:admin_dashboard")
                        + "?tab=forecast"
                        + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else "")
                    )

                # Append the new month entry to the existing forecast
                monthly.append(
                    {
                        "date": date_str,
                        "unit_price": unit_price,
                        "quantity": quantity,
                    }
                )
                existing_forecast.monthly_forecasts = monthly
                existing_forecast.part_name = part_name
                existing_forecast.save()
                forecast = existing_forecast
            else:
                monthly_forecast = [
                    {
                        "date": date_str,
                        "unit_price": unit_price,
                        "quantity": quantity,
                    }
                ]

                forecast = Forecast.objects.create(
                    customer=customer,
                    part_number=part_number,
                    part_name=part_name,
                    monthly_forecasts=monthly_forecast,
                )

            customer_parts = customer.parts or []
            part_exists = any(
                isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number
                for p in customer_parts
            )
            if not part_exists:
                customer_parts.append({"Partcode": part_number, "Partname": part_name})
                customer.parts = customer_parts
                customer.save()

            messages.success(request, f"Forecast added successfully for {customer_name} - {part_number}")
            return redirect(reverse("app:admin_dashboard") + "?tab=forecast" + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else ""))

        if action == "update_forecast":
            original_customer = _normalize_space(request.POST.get("original_customer_name"))
            original_part_number = _normalize_space(request.POST.get("original_part_number"))
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_name = _normalize_space(request.POST.get("part_name"))
            part_number = _normalize_space(request.POST.get("part_number"))
            month = request.POST.get("month")
            year = request.POST.get("year")
            unit_price = request.POST.get("unit_price")
            quantity = request.POST.get("quantity")
            original_date = (request.POST.get("original_date") or "").strip()

            if not original_customer or not original_part_number:
                messages.error(request, "Original customer and part number are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not customer_name:
                messages.error(request, "Customer name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_name:
                messages.error(request, "Part name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_number:
                messages.error(request, "Part number is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not unit_price:
                messages.error(request, "Unit price is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not quantity:
                messages.error(request, "Quantity is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            try:
                unit_price = float(unit_price)
                quantity = float(quantity)
            except ValueError:
                messages.error(request, "Unit price and quantity must be valid numbers.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Find the original customer
            original_customer_obj = Customer.objects.filter(customer_name__iexact=original_customer).first()
            if not original_customer_obj:
                messages.error(request, f"Original customer '{original_customer}' not found.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Find the forecast to update
            forecast = Forecast.objects.filter(
                customer=original_customer_obj,
                part_number=original_part_number
            ).first()

            if not forecast:
                messages.error(request, f"Forecast not found for {original_customer} - {original_part_number}")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Handle customer change if needed
            if customer_name != original_customer:
                new_customer, created = Customer.objects.get_or_create(
                    customer_name=customer_name,
                    defaults={"parts": []}
                )
                forecast.customer = new_customer
            else:
                forecast.customer = original_customer_obj

            # Update basic fields
            forecast.part_number = part_number
            forecast.part_name = part_name

            # We always expect month/year for per‑month editing
            if not month or not year:
                messages.error(request, "Month and year are required to update a forecast.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            date_str = f"{month}-{year}"

            # Update only the targeted monthly entry (identified by original_date)
            monthly_list = list(forecast.monthly_forecasts or [])
            updated = False
            original_date_normalized = original_date.lower()

            for entry in monthly_list:
                if not isinstance(entry, dict):
                    continue
                existing_date = str(entry.get("date", "")).strip()
                if original_date and existing_date.lower() == original_date_normalized:
                    entry["date"] = date_str
                    entry["unit_price"] = unit_price
                    entry["quantity"] = quantity
                    updated = True
                    break

            if not updated:
                # If we didn't find the original month, append as a new one
                monthly_list.append(
                    {
                        "date": date_str,
                        "unit_price": unit_price,
                        "quantity": quantity,
                    }
                )

            # Prevent duplicate month/year entries for the same forecast
            seen_dates = set()
            deduped = []
            for entry in monthly_list:
                if not isinstance(entry, dict):
                    continue
                d = str(entry.get("date", "")).strip()
                key = d.lower()
                if key and key not in seen_dates:
                    seen_dates.add(key)
                    deduped.append(entry)

            forecast.monthly_forecasts = deduped

            # Save the forecast
            forecast.save()

            # Update customer.parts for both old and new customers
            if customer_name != original_customer:
                # Remove from old customer's parts if no other forecasts use it
                other_forecasts = Forecast.objects.filter(
                    customer=original_customer_obj,
                    part_number=original_part_number
                ).exclude(id=forecast.id).exists()
                
                if not other_forecasts:
                    old_parts = original_customer_obj.parts or []
                    updated_parts = [
                        p for p in old_parts 
                        if not (isinstance(p, dict) and str(p.get("Partcode", "")).strip() == original_part_number)
                    ]
                    original_customer_obj.parts = updated_parts
                    original_customer_obj.save()
                
                # Add to new customer's parts
                new_customer = forecast.customer
                new_parts = new_customer.parts or []
                part_exists = any(
                    isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number
                    for p in new_parts
                )
                if not part_exists:
                    new_parts.append({"Partcode": part_number, "Partname": part_name})
                    new_customer.parts = new_parts
                    new_customer.save()
            else:
                # Update part in same customer's parts if needed
                if part_number != original_part_number:
                    # Remove old part
                    old_parts = original_customer_obj.parts or []
                    updated_parts = [
                        p for p in old_parts 
                        if not (isinstance(p, dict) and str(p.get("Partcode", "")).strip() == original_part_number)
                    ]
                    
                    # Add new part if not exists
                    part_exists = any(
                        isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number
                        for p in updated_parts
                    )
                    if not part_exists:
                        updated_parts.append({"Partcode": part_number, "Partname": part_name})
                    
                    original_customer_obj.parts = updated_parts
                    original_customer_obj.save()

            messages.success(request, f"Forecast updated successfully for {customer_name} - {part_number}")
            return redirect(reverse("app:admin_dashboard") + "?tab=forecast" + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else ""))
        
        if action == "delete_forecast":
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_number = _normalize_space(request.POST.get("part_number"))
            forecast_id = (request.POST.get("forecast_id") or "").strip()
            date_str = (request.POST.get("date") or "").strip()

            if not customer_name or not part_number:
                messages.error(request, "Customer name and part number are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Find the customer
            customer = Customer.objects.filter(customer_name__iexact=customer_name).first()
            if not customer:
                messages.error(request, f"Customer '{customer_name}' not found.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # If a specific forecast id and date are provided, delete only that month
            if forecast_id and date_str:
                forecast = Forecast.objects.filter(
                    id=forecast_id,
                    customer=customer,
                    part_number=part_number,
                ).first()

                if not forecast:
                    messages.error(request, f"Forecast not found for {customer_name} - {part_number}")
                    return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

                monthly_list = list(forecast.monthly_forecasts or [])
                target = date_str.strip().lower()
                new_monthly = [
                    m
                    for m in monthly_list
                    if not (
                        isinstance(m, dict)
                        and str(m.get("date", "")).strip().lower() == target
                    )
                ]

                if not new_monthly:
                    # No more months left → delete the whole forecast
                    forecast.delete()
                else:
                    forecast.monthly_forecasts = new_monthly
                    forecast.save()

                # If no other forecasts remain for this part, clean up customer.parts
                other_forecasts = Forecast.objects.filter(
                    customer=customer,
                    part_number=part_number,
                ).exists()

                if not other_forecasts:
                    customer_parts = customer.parts or []
                    updated_parts = [
                        p
                        for p in customer_parts
                        if not (
                            isinstance(p, dict)
                            and str(p.get("Partcode", "")).strip() == part_number
                        )
                    ]
                    customer.parts = updated_parts
                    customer.save()

                messages.success(
                    request,
                    f"Forecast month deleted successfully: {customer_name} - {part_number} ({date_str})",
                )
                return redirect(
                    reverse("app:admin_dashboard")
                    + "?tab=forecast"
                    + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else "")
                )

            # Fallback: delete all forecasts for this customer/part if no specific month given
            forecasts_qs = Forecast.objects.filter(
                customer=customer,
                part_number=part_number
            )

            if not forecasts_qs.exists():
                messages.error(request, f"Forecast not found for {customer_name} - {part_number}")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            forecast_info = f"{customer_name} - {part_number}"
            forecasts_qs.delete()

            other_forecasts = Forecast.objects.filter(
                customer=customer,
                part_number=part_number
            ).exists()

            if not other_forecasts:
                customer_parts = customer.parts or []
                updated_parts = [
                    p for p in customer_parts 
                    if not (isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number)
                ]
                customer.parts = updated_parts
                customer.save()

            messages.success(request, f"Forecast deleted successfully: {forecast_info}")
            return redirect(
                reverse("app:admin_dashboard")
                + "?tab=forecast"
                + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else "")
            )

        if action == "toggle_user_admin":
            user_id = (request.POST.get("user_id") or "").strip()
            if not user_id:
                messages.error(request, "Missing user ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                u = User.objects.get(id=user_id)

                if u.id == request.user.id:
                    messages.error(request, "You can't change your own admin role here.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=users")

                if u.is_superuser:
                    u.is_superuser = False
                    u.is_staff = True
                    u.save(update_fields=["is_superuser", "is_staff"])
                    messages.success(request, f"{u.username} is now Staff.")
                else:
                    u.is_superuser = True
                    u.is_staff = True
                    u.save(update_fields=["is_superuser", "is_staff"])
                    messages.success(request, f"{u.username} is now Admin.")

            except User.DoesNotExist:
                messages.error(request, "User not found.")
            except Exception as e:
                messages.error(request, f"Failed to update role: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

        if action == "remove_staff":
            user_id = (request.POST.get("user_id") or "").strip()
            if not user_id:
                messages.error(request, "Missing user ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                u = User.objects.get(id=user_id)

                if u.id == request.user.id:
                    messages.error(request, "You can't delete your own account.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=users")

                try:
                    prof = getattr(u, "employeeprofile", None)
                    if prof is not None:
                        prof.delete()
                except Exception:
                    pass

                username = u.username
                u.delete()
                messages.success(request, f"Deleted user: {username}")

            except User.DoesNotExist:
                messages.error(request, "User not found.")
            except Exception as e:
                messages.error(request, f"Failed to delete user: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

    # ── GET: build context ────────────────────────────────────────────────────

    q = (request.GET.get("q") or "").strip()
    customers = build_customer_table(q)

    master_map = {
        m["mat_partcode"]: {
            "mat_partname": m["mat_partname"],
            "mat_maker": m["mat_maker"],
            "unit": m["unit"],
        }
        for m in MaterialList.objects.all().values("mat_partcode", "mat_partname", "mat_maker", "unit")
    }

    mq = (request.GET.get("mq") or "").strip()
    materials_qs = MaterialList.objects.all().order_by("mat_partcode")

    if mq:
        materials_qs = materials_qs.filter(
            Q(mat_partcode__icontains=mq) |
            Q(mat_partname__icontains=mq) |
            Q(mat_maker__icontains=mq) |
            Q(unit__icontains=mq)
        )

    paginator = Paginator(materials_qs, 8)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    material_total = materials_qs.count()
    material_list = page_obj

    uq = (request.GET.get("uq") or "").strip()
    users_qs = User.objects.all().order_by("-is_superuser", "-is_staff", "username")

    if uq:
        users_qs = users_qs.filter(
            Q(username__icontains=uq) |
            Q(employeeprofile__full_name__icontains=uq) |
            Q(employeeprofile__department__icontains=uq)
        )

    users_paginator = Paginator(users_qs, 10)
    upage = request.GET.get("upage")
    users_page = users_paginator.get_page(upage)
    user_total = users_qs.count()

    # Current Forecast Tab Data
    fq = (request.GET.get("fq") or "").strip()
    fcustomer = (request.GET.get("fcustomer") or "").strip()
    page_number = request.GET.get('page', 1)
    
    forecasts_qs = Forecast.objects.select_related("customer").order_by("-id")
    
    if fq:
        forecasts_qs = forecasts_qs.filter(
            Q(part_number__icontains=fq)
            | Q(part_name__icontains=fq)
            | Q(customer__customer_name__icontains=fq)
        )
    
    if fcustomer:
        forecasts_qs = forecasts_qs.filter(customer__customer_name=fcustomer)

    # Get total count before pagination
    forecasts_total = forecasts_qs.count()
    
    # Create paginator with 8 items per page
    paginator = Paginator(forecasts_qs, 8)
    forecasts_page = paginator.get_page(page_number)
    
    # Process the paginated forecasts
    forecasts_list = []
    for forecast in forecasts_page:
        first_monthly = None
        if forecast.monthly_forecasts and len(forecast.monthly_forecasts) > 0:
            first_monthly = forecast.monthly_forecasts[0]
            if isinstance(first_monthly, dict):
                first_monthly = {
                    "date": first_monthly.get("date", ""),
                    "unit_price": float(first_monthly.get("unit_price", 0)),
                    "quantity": float(first_monthly.get("quantity", 0)),
                }

        forecast.unit_price_display = first_monthly.get("unit_price", 0) if first_monthly else 0
        forecast.quantity_display = first_monthly.get("quantity", 0) if first_monthly else 0

        forecasts_list.append(forecast)

    forecasts_monthly_json = {}
    for f in forecasts_list:
        monthly_list = []
        for m in (f.monthly_forecasts or []):
            if isinstance(m, dict):
                monthly_list.append({
                    "date": m.get("date", ""),
                    "unit_price": float(m.get("unit_price", 0)),
                    "quantity": float(m.get("quantity", 0)),
                })
        forecasts_monthly_json[str(f.id)] = monthly_list

    forecasts_monthly_json = json.dumps(forecasts_monthly_json, default=str)

    # ── Previous Forecast Tab Data (NEW) ─────────────────────────────────────
    pf_customer = (request.GET.get("pf_customer") or "").strip()
    pf_q = (request.GET.get("pf_q") or "").strip()
    
    # Get current and previous years
    from datetime import date
    current_year = date.today().year
    previous_year = current_year - 1
    
    prev_data = {}
    if tab == "previous_forecast":
        # Month mapping for display
        month_map = {
            1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
            7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'
        }
        
        # Month name to number mapping for parsing
        month_name_to_num = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
            'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'sept': 9,
            'oct': 10, 'nov': 11, 'dec': 12
        }
        
        # Get all forecasts
        qs = Forecast.objects.select_related("customer").all()
        
        # Apply filters
        if pf_customer:
            qs = qs.filter(customer__customer_name=pf_customer)
        
        if pf_q:
            qs = qs.filter(
                Q(part_number__icontains=pf_q) | 
                Q(part_name__icontains=pf_q)
            )
        
        # Organize data by (customer, part_number)
        rows_by_key = {}
        total_qty = defaultdict(float)
        total_amt = defaultdict(float)
        
        for forecast in qs:
            monthly = forecast.monthly_forecasts or []
            
            # Get first entry for unit price
            first_entry = next(
                (m for m in monthly if isinstance(m, dict)),
                {}
            )
            
            try:
                unit_price = float(first_entry.get("unit_price", 0)) if isinstance(first_entry, dict) else 0.0
            except (TypeError, ValueError):
                unit_price = 0.0
            
            customer_name = forecast.customer.customer_name if forecast.customer else "—"
            key = (customer_name, forecast.part_number)
            
            # Initialize row if not exists
            if key not in rows_by_key:
                rows_by_key[key] = {
                    "customer": customer_name,
                    "part_number": forecast.part_number,
                    "part_name": forecast.part_name,
                    "unit_price": unit_price,
                    "months": defaultdict(float)
                }
            
            row = rows_by_key[key]
            
            # Process each monthly entry
            for entry in monthly:
                if not isinstance(entry, dict):
                    continue
                
                date_str = entry.get("date", "").strip()
                if not date_str:
                    continue
                
                # Parse date (format: "Month-YYYY")
                try:
                    parts = date_str.split('-')
                    if len(parts) < 2:
                        continue
                    
                    month_name = parts[0].strip().lower()
                    year = int(parts[-1].strip())
                    
                    # Only include if year is previous year
                    if year != previous_year:
                        continue
                    
                    month_num = month_name_to_num.get(month_name)
                    if not month_num:
                        continue
                    
                    month_abbr = month_map[month_num]
                    
                    # Get quantity
                    try:
                        qty = float(entry.get("quantity", 0) or 0)
                    except (TypeError, ValueError):
                        qty = 0.0
                    
                    # Add to row
                    row["months"][month_abbr] += qty
                    
                    # Add to totals
                    total_qty[month_abbr] += qty
                    total_amt[month_abbr] += qty * unit_price
                    
                except (ValueError, IndexError, KeyError):
                    continue
        
        # Convert defaultdict to regular dict for template
        prev_rows = []
        for key, row in rows_by_key.items():
            row["months"] = dict(row["months"])
            prev_rows.append(row)
        
        # Sort rows by customer then part number
        prev_rows.sort(key=lambda x: (x["customer"], x["part_number"]))
        
        # Get unique customers for filter dropdown
        prev_customers = list(set(
            Forecast.objects.filter(
                monthly_forecasts__0__date__icontains=str(previous_year)
            ).values_list("customer__customer_name", flat=True).distinct().order_by("customer__customer_name")
        ))
        
        prev_data = {
            "prev_rows": prev_rows,
            "prev_total_qty": dict(total_qty),
            "prev_total_amt": dict(total_amt),
            "prev_customers": prev_customers,
            "pf_customer": pf_customer,
            "pf_q": pf_q,
            "fs_prev_months": ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                               'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
            "fs_prev_year": previous_year,
        }

    # ── Forecast Summary tab data ─────────────────────────────────────────────
    fsq = (request.GET.get("fsq") or "").strip()
    fsq_customer = (request.GET.get("fsq_customer") or "").strip()

    fs_data = {}
    if tab == "forecast_summary":
        fs_data = _build_forecast_summary(fsq=fsq, fsq_customer=fsq_customer)

    # Actual Delivered tab data
    adq = (request.GET.get("adq") or "").strip()
    ad_customer = (request.GET.get("ad_customer") or "").strip()

    ad_data = {}
    if tab == "actual_delivered":
        ad_data = _build_actual_summary(adq=adq, ad_customer=ad_customer)

    tep_id = request.GET.get("tep_id")
    is_ajax = request.headers.get("x-requested-with") == "XMLHttpRequest"

    if tep_id and is_ajax:
        tep = get_object_or_404(TEPCode.objects.select_related("customer"), id=tep_id)
        materials = Material.objects.filter(tep_code=tep).order_by("mat_partname")

        selected_part = (tep.part_code or "").strip()
        selected_part_name = ""

        for p in (tep.customer.parts or []):
            if isinstance(p, dict) and str(p.get("Partcode", "")).strip() == selected_part:
                selected_part_name = str(p.get("Partname", "")).strip()
                break

        return render(request, "admin/_customer_detail_panel.html", {
            "customer": tep.customer,
            "materials": materials,
            "selected_tep": tep.tep_code,
            "selected_part": selected_part,
            "selected_part_name": selected_part_name,
            "tep_id": tep.id,
        })

    # ── Build final context ───────────────────────────────────────────────────
    context = {
        "tab": tab,

        "customers_count": Customer.objects.count(),
        "tep_count": TEPCode.objects.count(),
        "materials_count": Material.objects.count(),
        "users_count": User.objects.count(),
        "forecasts_count": Forecast.objects.count(),

        "customers": customers,
        "q": q,

        "mq": mq,
        "material_total": material_total,
        "material_list": material_list,
        "page_obj": page_obj,

        "uq": uq,
        "user_total": user_total,
        "users_page": users_page,

        "fq": fq,
        "fcustomer": fcustomer,
        "forecasts_list": forecasts_page,
        "forecasts_total": forecasts_total,
        "forecasts_monthly_json": forecasts_monthly_json,
        "all_customers": Customer.objects.all().order_by("customer_name"),
        "forecast_customers": Customer.objects.filter(forecasts__isnull=False).distinct().order_by("customer_name"),

        "master_map_json": json.dumps(master_map, ensure_ascii=False),

        # Forecast Summary
        "fsq":         fsq,
        "fsq_customer": fsq_customer,
        **fs_data,
        
        # Previous Forecast
        **prev_data,

        # Actual Delivered
        "adq": adq,
        "ad_customer": ad_customer,
        **ad_data,
    }
    
    return render(request, "admin/dashboard.html", context)


@login_required
@user_passes_test(is_admin)
def admin_users(request):
    return redirect(reverse("app:admin_dashboard") + "?tab=users")


@login_required
@user_passes_test(is_admin)
def toggle_user_active(request, user_id):
    user_obj = get_object_or_404(User, id=user_id)

    if user_obj == request.user:
        messages.error(request, "You can't disable your own account.")
        return redirect(reverse("app:admin_dashboard") + "?tab=users")

    user_obj.is_active = not user_obj.is_active
    user_obj.save(update_fields=["is_active"])

    messages.success(request, f"Updated user: {user_obj.username} (active={user_obj.is_active})")
    return redirect(reverse("app:admin_dashboard") + "?tab=users")


@login_required
@user_passes_test(is_admin)
def create_employee(request):
    if request.method == "POST":
        form = EmployeeCreateForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Employee account created successfully.")
            return redirect(reverse("app:admin_dashboard") + "?tab=users")
    else:
        form = EmployeeCreateForm()

    return render(request, "create_employee.html", {"form": form})


@login_required
@user_passes_test(is_admin)
def admin_csv_upload(request):
    default_next = reverse("app:admin_dashboard") + "?tab=materials"
    next_url = request.POST.get("next") or request.GET.get("next") or default_next

    if not url_has_allowed_host_and_scheme(next_url, allowed_hosts={request.get_host()}):
        next_url = default_next

    if request.method == "POST" and request.FILES.get("csv_file"):
        f = request.FILES["csv_file"]
        raw = f.read()

        content = None
        for enc in ("utf-8-sig", "utf-16", "cp1252", "latin-1"):
            try:
                content = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue

        if content is None:
            messages.error(request, "Could not read file encoding. Save as CSV UTF-8 and upload again.")
            return redirect(next_url)

        csv_file = io.StringIO(content)
        reader = csv.DictReader(csv_file)
        reader.fieldnames = [h.strip().lstrip("\ufeff") for h in (reader.fieldnames or [])]

        master_inserted = 0
        master_updated = 0
        ALLOWED_UNITS = {"pc", "pcs", "m", "g", "kg"}

        def sget(row, *keys, default=""):
            for k in keys:
                v = row.get(k)
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            return default

        try:
            with transaction.atomic():
                for row in reader:
                    mat_partcode = sget(row, "mat_partcode", "material_part_code")
                    mat_partname = sget(row, "mat_partname", "material_name")
                    mat_maker = sget(row, "mat_maker", "maker")
                    unit = sget(row, "unit", default="pc").lower()

                    if unit not in ALLOWED_UNITS:
                        unit = "pc"

                    if not mat_partcode:
                        continue

                    master, created_master = MaterialList.objects.get_or_create(
                        mat_partcode=mat_partcode,
                        defaults={
                            "mat_partname": mat_partname or mat_partcode,
                            "mat_maker": mat_maker or "Unknown",
                            "unit": unit,
                        }
                    )

                    if created_master:
                        master_inserted += 1
                    else:
                        changed = False
                        if mat_partname and master.mat_partname != mat_partname:
                            master.mat_partname = mat_partname
                            changed = True
                        if mat_maker and master.mat_maker != mat_maker:
                            master.mat_maker = mat_maker
                            changed = True
                        if unit and master.unit != unit:
                            master.unit = unit
                            changed = True
                        if changed:
                            master.save()
                            master_updated += 1

            messages.success(
                request,
                f"CSV uploaded successfully | master_inserted={master_inserted}, master_updated={master_updated}"
            )
            return redirect(next_url)

        except Exception as e:
            messages.error(request, f"Upload failed: {e}")
            return redirect(next_url)

    return redirect(next_url)


@login_required
@user_passes_test(is_admin)
def admin_forecast_csv_upload(request):
    """
    Upload a CSV file containing forecast data.

    Expected columns (case-insensitive, flexible):
      - customer_name / CUSTOMER
      - part_number / Partcode / PART_NUMBER
      - part_name / Partname / PART_NAME
      - Either:
          * date  (e.g. "January-2025" or "Jan-2025"), or
          * month + year columns which will be combined as "Month-Year"
      - unit_price
      - quantity

    Each row represents one monthly forecast entry. Rows with the same
    (customer_name, part_number, part_name) are grouped into a single
    Forecast record whose monthly_forecasts list contains all months
    from the CSV.
    """
    default_next = reverse("app:admin_dashboard") + "?tab=forecast"
    next_url = request.POST.get("next") or request.GET.get("next") or default_next

    if not url_has_allowed_host_and_scheme(next_url, allowed_hosts={request.get_host()}):
        next_url = default_next

    if request.method == "POST" and request.FILES.get("csv_file"):
        f = request.FILES["csv_file"]
        raw = f.read()

        content = None
        for enc in ("utf-8-sig", "utf-16", "cp1252", "latin-1"):
            try:
                content = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue

        if content is None:
            messages.error(request, "Could not read file encoding. Save as CSV UTF-8 and upload again.")
            return redirect(next_url)

        def fnum(val, default=0.0):
            try:
                if val is None:
                    return float(default)
                s = str(val).strip()
                if s == "":
                    return float(default)
                s = s.replace(",", "").replace(" ", "")
                if s == "-" or s == "":
                    return float(default)
                return float(s)
            except Exception:
                return float(default)

        # Try to detect the special 2-row banded header format
        csv_file = io.StringIO(content)
        rows = list(csv.reader(csv_file))

        def _is_wide_band_format(all_rows):
            if len(all_rows) < 3:
                return False
            band_line = " ".join([c or "" for c in all_rows[0]]).upper()
            return "ACTUAL DELIVERED" in band_line and "PREVIOUS FORECAST" in band_line

        grouped = defaultdict(list)

        if _is_wide_band_format(rows):
            header_band = rows[0]
            header_cols = rows[1]
            data_rows = rows[2:]

            def _match_col(names):
                for idx, col in enumerate(header_cols):
                    name = (col or "").strip().lower()
                    if name in names:
                        return idx
                return None

            idx_customer = _match_col({"customer", "customer_name"})
            idx_part_no = _match_col({"part number", "part_number", "partcode", "part code"})
            idx_part_name = _match_col({"part name", "part_name", "partname"})
            idx_unit_price = _match_col({"unit price", "unit_price", "unitprice"})

            if idx_customer is None or idx_part_no is None or idx_part_name is None:
                messages.error(request, "CSV header missing Customer / Part number / Part name.")
                return redirect(next_url)

            import re
            band_info = []
            for band_raw, col_raw in zip(header_band, header_cols):
                band_label = (band_raw or "").strip().upper()
                col_label = (col_raw or "").strip().upper()

                if not band_label or not col_label:
                    band_info.append(None)
                    continue

                group = None
                if "ACTUAL DELIVERED" in band_label:
                    group = "actual"
                elif "PREVIOUS FORECAST" in band_label:
                    group = "prev"
                elif "FORECAST" in band_label and "PREVIOUS" not in band_label:
                    group = "fore"
                else:
                    band_info.append(None)
                    continue

                m = re.search(r"(\d{4})", band_label)
                year_from_band = int(m.group(1)) if m else None

                month_map = {
                    "JAN": "January",
                    "FEB": "February",
                    "MAR": "March",
                    "APR": "April",
                    "MAY": "May",
                    "JUN": "June",
                    "JUL": "July",
                    "AUG": "August",
                    "SEP": "September",
                    "SEPT": "September",
                    "OCT": "October",
                    "NOV": "November",
                    "DEC": "December",
                }
                key3 = col_label[:3]
                month_full = month_map.get(key3)
                if not month_full:
                    band_info.append(None)
                    continue

                band_info.append(
                    {
                        "group": group,
                        "month_full": month_full,
                        "year_from_band": year_from_band,
                    }
                )

            from datetime import date as _date_cls
            actual_years = [
                info["year_from_band"]
                for info in band_info
                if info and info["group"] == "actual" and info["year_from_band"]
            ]
            actual_year = actual_years[0] if actual_years else _date_cls.today().year
            prev_year = actual_year - 1

            for row_vals in data_rows:
                if len(row_vals) < len(header_cols):
                    row_vals = row_vals + [""] * (len(header_cols) - len(row_vals))

                customer_name = (row_vals[idx_customer] or "").strip()
                part_number = (row_vals[idx_part_no] or "").strip()
                part_name = (row_vals[idx_part_name] or "").strip()

                if not (customer_name and part_number and part_name):
                    continue

                unit_price = 0.0
                if idx_unit_price is not None and idx_unit_price < len(row_vals):
                    unit_price = fnum(row_vals[idx_unit_price], 0.0)

                key = (customer_name, part_number, part_name)
                date_map = {}

                for col_idx, info in enumerate(band_info):
                    if not info or col_idx >= len(row_vals):
                        continue

                    qty = fnum(row_vals[col_idx], 0.0)
                    if not qty:
                        continue

                    group = info["group"]
                    month_full = info["month_full"]

                    if group == "actual":
                        year = info["year_from_band"] or actual_year
                    elif group == "prev":
                        year = prev_year
                    else:
                        year = actual_year

                    date_str = f"{month_full}-{year}"

                    entry = date_map.get(date_str)
                    if not entry:
                        entry = {
                            "date": date_str,
                            "unit_price": unit_price,
                        }
                        date_map[date_str] = entry

                    if group == "actual":
                        entry["actual_quantity"] = entry.get("actual_quantity", 0.0) + qty
                    elif group == "prev":
                        entry["prev_quantity"] = entry.get("prev_quantity", 0.0) + qty
                    elif group == "fore":
                        entry["quantity"] = entry.get("quantity", 0.0) + qty

                if not date_map:
                    continue

                grouped[key].extend(list(date_map.values()))

        else:
            # Fallback: original DictReader-based handling
            csv_file = io.StringIO(content)
            reader = csv.DictReader(csv_file)
            reader.fieldnames = [h.strip().lstrip("\ufeff") for h in (reader.fieldnames or [])]

            def sget(row, *keys, default=""):
                for k in keys:
                    v = row.get(k)
                    if v is not None and str(v).strip() != "":
                        return str(v).strip()
                return default

            for row in reader:
                customer_name = sget(row, "customer_name", "Customer", "CUSTOMER")
                part_number = sget(row, "part_number", "Partcode", "PART_NUMBER", "part_code")
                part_name = sget(row, "part_name", "Partname", "PART_NAME")

                unit_price = fnum(row.get("unit_price") or row.get("UnitPrice") or row.get("price"), 0.0)
                base_quantity = fnum(row.get("quantity") or row.get("qty") or row.get("Quantity"), 0.0)

                if not (customer_name and part_number and part_name):
                    continue

                key = (customer_name, part_number, part_name)

                date_str = sget(row, "date", "month_year", "MonthYear")
                month = sget(row, "month", "Month")
                year = sget(row, "year", "Year")

                if not date_str and (month or year):
                    if month and year:
                        date_str = f"{month}-{year}"
                    elif month:
                        from datetime import date
                        date_str = f"{month}-{date.today().year}"

                if date_str and base_quantity:
                    grouped[key].append(
                        {
                            "date": date_str,
                            "unit_price": unit_price,
                            "quantity": base_quantity,
                        }
                    )

                month_columns = [
                    ("January", ["JAN", "Jan", "January"]),
                    ("February", ["FEB", "Feb", "February"]),
                    ("March", ["MAR", "Mar", "March"]),
                    ("April", ["APR", "Apr", "April"]),
                    ("May", ["MAY", "May"]),
                    ("June", ["JUN", "Jun", "June"]),
                    ("July", ["JUL", "Jul", "July"]),
                    ("August", ["AUG", "Aug", "August"]),
                    ("September", ["SEP", "Sept", "SEPT", "September"]),
                    ("October", ["OCT", "Oct", "October"]),
                    ("November", ["NOV", "Nov", "November"]),
                    ("December", ["DEC", "Dec", "December"]),
                ]

                from datetime import date as _date_cls
                wide_year_raw = sget(row, "forecast_year", "year_forecast", "year", "Year")
                try:
                    wide_year = int(wide_year_raw) if wide_year_raw else _date_cls.today().year
                except ValueError:
                    wide_year = _date_cls.today().year

                for full_name, aliases in month_columns:
                    header = None
                    for alias in aliases:
                        if alias in row:
                            header = alias
                            break
                    if not header:
                        continue

                    qty_val = fnum(row.get(header), 0.0)
                    if not qty_val:
                        continue

                    grouped[key].append(
                        {
                            "date": f"{full_name}-{wide_year}",
                            "unit_price": unit_price,
                            "quantity": qty_val,
                        }
                    )

        if not grouped:
            messages.error(request, "No valid forecast rows found in CSV.")
            return redirect(next_url)

        created_count = 0
        updated_count = 0

        try:
            with transaction.atomic():
                for (cust_name, part_no, part_nm), monthly in grouped.items():
                    customer, _ = Customer.objects.get_or_create(
                        customer_name=cust_name,
                        defaults={"parts": []}
                    )

                    forecast = Forecast.objects.filter(
                        customer=customer,
                        part_number=part_no
                    ).first()

                    if forecast:
                        forecast.part_name = part_nm or forecast.part_name
                        forecast.monthly_forecasts = monthly
                        forecast.save()
                        updated_count += 1
                    else:
                        Forecast.objects.create(
                            customer=customer,
                            part_number=part_no,
                            part_name=part_nm,
                            monthly_forecasts=monthly,
                        )
                        created_count += 1

                    # Ensure the part exists in customer.parts
                    parts = customer.parts or []
                    exists = any(
                        isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_no
                        for p in parts
                    )
                    if not exists:
                        parts.append({"Partcode": part_no, "Partname": part_nm})
                        customer.parts = parts
                        customer.save(update_fields=["parts"])

            messages.success(
                request,
                f"Forecast CSV uploaded successfully | created={created_count}, updated={updated_count}"
            )
        except Exception as e:
            messages.error(request, f"Forecast CSV upload failed: {e}")

        return redirect(next_url)

    return redirect(next_url)


@login_required
def customer_list(request):
    q = (request.GET.get("q") or "").strip()
    customers = build_customer_table(q)
    return render(request, "customer_list.html", {"customers": customers, "q": q})


@never_cache
@login_required
def customer_detail(request, tep_id: int):
    tep = get_object_or_404(
        TEPCode.objects.select_related("customer"),
        id=tep_id
    )

    materials = (
        Material.objects
        .filter(tep_code=tep)
        .order_by("mat_partname")
    )

    selected_part = (tep.part_code or "").strip()
    selected_part_name = ""

    for p in (tep.customer.parts or []):
        if isinstance(p, dict) and str(p.get("Partcode", "")).strip() == selected_part:
            selected_part_name = str(p.get("Partname", "")).strip()
            break

    return render(request, "customer_detail.html", {
        "customer": tep.customer,
        "materials": materials,
        "selected_tep": tep.tep_code,
        "selected_part": selected_part,
        "selected_part_name": selected_part_name,
        "tep_id": tep.id,
    })


@login_required
@user_passes_test(is_admin)
def add_material_to_tep(request):
    if request.method != "POST":
        return redirect("app:admin_dashboard")

    tep_id = (request.POST.get("tep_id") or "").strip()
    mat_partcode = _normalize_space(request.POST.get("mat_partcode"))
    dim_qty_raw = (request.POST.get("dim_qty") or "").strip()
    loss_raw = (request.POST.get("loss_percent") or "").strip()

    if not tep_id:
        messages.error(request, "Missing TEP id.")
        return redirect("app:admin_dashboard")

    if not mat_partcode:
        messages.error(request, "Material Part Code is required.")
        return redirect("app:admin_dashboard")

    if not dim_qty_raw:
        messages.error(request, "Dim/Qty is required.")
        return redirect("app:admin_dashboard")

    try:
        dim_qty = float(dim_qty_raw)
    except Exception:
        messages.error(request, "Dim/Qty must be a number.")
        return redirect("app:admin_dashboard")

    loss_percent = 10.0
    if loss_raw != "":
        try:
            loss_percent = float(loss_raw)
        except Exception:
            messages.error(request, "Loss % must be a number.")
            return redirect("app:admin_dashboard")

    tep = get_object_or_404(TEPCode, id=tep_id)

    master = MaterialList.objects.filter(mat_partcode=mat_partcode).first()
    if not master:
        messages.error(request, f"mat_partcode not found in master list: {mat_partcode}")
        return redirect("app:admin_dashboard")

    total = round(float(dim_qty) * (1 + (float(loss_percent) / 100.0)), 4)

    try:
        with transaction.atomic():
            final_name = _allocate_material_name(
                tep=tep,
                base_name=master.mat_partname,
                exclude_partcode=mat_partcode
            )

            material, created = Material.objects.get_or_create(
                tep_code=tep,
                mat_partcode=mat_partcode,
                defaults={
                    "mat_partname": final_name,
                    "mat_maker": master.mat_maker,
                    "unit": master.unit,
                    "dim_qty": dim_qty,
                    "loss_percent": loss_percent,
                    "total": total,
                }
            )

            if not created:
                messages.error(request, f"Material already exists for this TEP + {mat_partcode}.")
            else:
                messages.success(request, f"Added material: {mat_partcode}")

    except Exception as e:
        messages.error(request, f"Failed to add material: {e}")

    return redirect(reverse("app:admin_dashboard") + "?tab=customers")


def logout_view(request):
    logout(request)
    return redirect(reverse("app:login"))
