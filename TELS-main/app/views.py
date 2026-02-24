
# views.py
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
    """
    Desired behavior per TEP:
      - First insert:        BASE
      - Second insert:       (rename existing BASE -> BASE 1), new -> BASE 2
      - Third insert:        new -> BASE 3
    """
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


def _month_index_from_string(val: str) -> int | None:
    """
    Convert various month representations (Jan-2026, JAN, January, 1, 01/2026) to 1-12.
    Returns None if it cannot be parsed.
    """
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None

    months = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    abbr = ["jan", "feb", "mar", "apr", "may", "jun",
            "jul", "aug", "sep", "oct", "nov", "dec"]

    lower = s.lower()
    for i, a in enumerate(abbr):
        if lower.startswith(a) or lower == a:
            return i + 1

    for i, name in enumerate(months):
        if lower.startswith(name) or lower == name:
            return i + 1

    try:
        head = s.split("-")[0] if "-" in s else s.split("/")[0] if "/" in s else s
        n = int(head)
        if 1 <= n <= 12:
            return n
    except (ValueError, IndexError):
        return None

    return None


def _range_totals(monthly_rows, from_idx: int, to_idx: int) -> tuple[float, float]:
    """
    Compute (total_quantity, total_amount) across monthly rows limited to
    months between from_idx and to_idx inclusive, where indexes are 1-12.
    """
    total_qty = 0.0
    total_amt = 0.0
    if from_idx is None or to_idx is None:
        return total_qty, total_amt

    for m in monthly_rows or []:
        if not isinstance(m, dict):
            continue
        mi = _month_index_from_string(m.get("date", ""))
        if mi is None:
            continue
        if from_idx <= mi <= to_idx:
            try:
                qty = float(m.get("quantity", 0) or 0)
                price = float(m.get("unit_price", 0) or 0)
            except (TypeError, ValueError):
                continue
            total_qty += qty
            total_amt += price * qty
    return total_qty, total_amt


def _months_label_from_rows(monthly_rows):
    """Return comma-separated month names derived from a list of {date,...} dicts."""
    month_names_full = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    seen = set()
    names = []
    for m in monthly_rows or []:
        if not isinstance(m, dict):
            continue
        idx = _month_index_from_string(m.get("date", ""))
        if idx is not None and 1 <= idx <= 12:
            name = month_names_full[idx - 1]
        else:
            name = str(m.get("date", "")).strip()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return ", ".join(names) if names else "—"


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

        if action == "update_forecast":
            forecast_id = (request.POST.get("forecast_id") or "").strip()
            part_number = _normalize_space(request.POST.get("part_number"))
            part_name = _normalize_space(request.POST.get("part_name"))
            customer_id_raw = (request.POST.get("customer_id") or "").strip()
            monthly_raw = (request.POST.get("monthly_forecasts") or "").strip()

            if not forecast_id:
                messages.error(request, "Missing forecast ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_number:
                messages.error(request, "Part Number is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_name:
                messages.error(request, "Part Name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            try:
                forecast = Forecast.objects.get(id=forecast_id)
                forecast.part_number = part_number
                forecast.part_name = part_name
                forecast.customer_id = int(customer_id_raw) if customer_id_raw else None

                if monthly_raw:
                    try:
                        monthly = json.loads(monthly_raw)
                        if isinstance(monthly, list):
                            forecast.monthly_forecasts = monthly
                        else:
                            messages.warning(request, "Monthly forecasts must be a JSON array. Changes saved without monthly data.")
                    except json.JSONDecodeError as e:
                        messages.error(request, f"Invalid JSON for monthly forecasts: {e}")
                        return redirect(reverse("app:admin_dashboard") + "?tab=forecast&fq=" + (request.GET.get("fq", "")))

                forecast.save()
                messages.success(request, f"Saved forecast: {part_number}")

            except Forecast.DoesNotExist:
                messages.error(request, "Forecast not found.")
            except (ValueError, TypeError) as e:
                messages.error(request, f"Invalid data: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=forecast" + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else ""))

        if action in ("save_previous_forecast", "save_actual_delivered"):
            forecast_id = (request.POST.get("forecast_id") or "").strip()
            monthly_raw = (request.POST.get("monthly_forecasts") or "").strip()

            if not forecast_id:
                messages.error(request, "Select a forecast / part first.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            try:
                forecast = Forecast.objects.get(id=forecast_id)
            except Forecast.DoesNotExist:
                messages.error(request, "Forecast not found.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            monthly = []
            if monthly_raw:
                try:
                    data = json.loads(monthly_raw)
                except json.JSONDecodeError as e:
                    messages.error(request, f"Invalid JSON: {e}")
                    return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

                if not isinstance(data, list):
                    messages.error(request, "Monthly data must be a JSON array.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

                monthly = data

            if action == "save_previous_forecast":
                forecast.previous_forecasts = monthly
                field_label = "Previous forecast"
            else:
                forecast.actual_delivered = monthly
                field_label = "Actual delivered"

            forecast.save()
            messages.success(request, f"{field_label} saved for {forecast.part_number}.")
            return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

        if action == "delete_material":
            mat_id = (request.POST.get("mat_id") or "").strip()

            if not mat_id:
                messages.error(request, "Missing material ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=materials")

            try:
                obj = MaterialList.objects.get(id=mat_id)
                code = obj.mat_partcode
                obj.delete()
                messages.success(request, f"Deleted material: {code}")
            except MaterialList.DoesNotExist:
                messages.error(request, "Material not found.")
            except Exception as e:
                messages.error(request, f"Failed to delete: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=materials")

        if action == "add_employee":
            employee_id = (request.POST.get("employee_id") or "").strip()
            full_name = (request.POST.get("full_name") or "").strip()
            department = (request.POST.get("department") or "").strip()
            password = (request.POST.get("password") or "")

            if not employee_id or not full_name or not department or not password:
                messages.error(request, "All fields are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            if User.objects.filter(username=employee_id).exists():
                messages.error(request, f"Employee ID already exists: {employee_id}")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                user = User.objects.create_user(username=employee_id, password=password)
                user.is_staff = True
                user.is_superuser = False
                user.save()

                # create employeeprofile
                try:
                    from .models import EmployeeProfile
                    EmployeeProfile.objects.create(
                        user=user,
                        employee_id=employee_id,
                        full_name=full_name,
                        department=department
                    )
                except Exception:
                    pass

                messages.success(request, f"Employee created: {employee_id}")

            except Exception as e:
                messages.error(request, f"Failed to create employee: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

        if action == "toggle_user_active":
            user_id = (request.POST.get("user_id") or "").strip()
            if not user_id:
                messages.error(request, "Missing user ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                u = User.objects.get(id=user_id)

                if u.id == request.user.id:
                    messages.error(request, "You can't disable your own account.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=users")

                u.is_active = not u.is_active
                u.save(update_fields=["is_active"])
                messages.success(request, f"Updated user: {u.username} (active={u.is_active})")
            except User.DoesNotExist:
                messages.error(request, "User not found.")
            except Exception as e:
                messages.error(request, f"Failed to update user: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

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

    fq = (request.GET.get("fq") or "").strip()
    forecast_from_month = (request.GET.get("from_month") or "").strip()
    forecast_to_month = (request.GET.get("to_month") or "").strip()

    forecasts_qs = Forecast.objects.select_related("customer").order_by("part_number")
    if fq:
        forecasts_qs = forecasts_qs.filter(
            Q(part_number__icontains=fq)
            | Q(part_name__icontains=fq)
            | Q(customer__customer_name__icontains=fq)
        )
    forecasts_list = list(forecasts_qs)
    forecasts_total = len(forecasts_list)

    previous_forecasts_list = []
    for f in forecasts_list:
        rows = f.previous_forecasts or []
        if not rows:
            continue
        unit_price = 0.0
        latest_qty = 0.0
        total_qty_prev = 0.0
        total_amt_prev = 0.0
        for idx, m in enumerate(rows):
            if not isinstance(m, dict):
                continue
            try:
                price = float(m.get("unit_price", 0) or 0)
                qty = float(m.get("quantity", 0) or 0)
            except (TypeError, ValueError):
                continue
            if unit_price == 0.0:
                unit_price = price
            latest_qty = qty
            total_qty_prev += qty
            total_amt_prev += price * qty
        previous_forecasts_list.append(
            {
                "part_number": f.part_number,
                "part_name": f.part_name,
                "months": _months_label_from_rows(rows),
                "unit_price": unit_price,
                "quantity": latest_qty,
                "total_quantity": total_qty_prev,
                "total_amount": total_amt_prev,
            }
        )

    month_names_full = [
        "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
        "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
    ]
    forecast_selected_range_label = None
    forecast_selected_total_qty = None
    forecast_selected_total_amount = None

    from_idx = _month_index_from_string(forecast_from_month) if forecast_from_month else None
    to_idx = _month_index_from_string(forecast_to_month) if forecast_to_month else None
    if from_idx is not None and to_idx is not None:
        if from_idx > to_idx:
            from_idx, to_idx = to_idx, from_idx
        forecast_selected_range_label = f"{month_names_full[from_idx - 1]} to {month_names_full[to_idx - 1]}"

        total_qty_all = 0.0
        total_amt_all = 0.0
        for f in forecasts_list:
            qty, amt = _range_totals(f.monthly_forecasts, from_idx, to_idx)
            setattr(f, "selected_range_qty", qty)
            setattr(f, "selected_range_amount", amt)
            total_qty_all += qty
            total_amt_all += amt
        forecast_selected_total_qty = total_qty_all
        forecast_selected_total_amount = total_amt_all

    forecasts_monthly_json = json.dumps(
        {str(f.id): (f.monthly_forecasts or []) for f in forecasts_list},
        default=str,
    )

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
        "forecasts_list": forecasts_list,
        "forecasts_total": forecasts_total,
        "forecasts_monthly_json": forecasts_monthly_json,
        "previous_forecasts_list": previous_forecasts_list,
        "forecast_month_choices": [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ],
        "forecast_from_month": forecast_from_month,
        "forecast_to_month": forecast_to_month,
        "forecast_selected_range_label": forecast_selected_range_label,
        "forecast_selected_total_qty": forecast_selected_total_qty,
        "forecast_selected_total_amount": forecast_selected_total_amount,
        "all_customers": Customer.objects.all().order_by("customer_name"),
        "forecast_customers": Customer.objects.filter(forecasts__isnull=False).distinct().order_by("customer_name"),

        "master_map_json": json.dumps(master_map, ensure_ascii=False),
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
