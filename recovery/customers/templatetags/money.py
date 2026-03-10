from django import template

register = template.Library()

@register.filter
def money(value):
    """Format integer-like values with thousands separators."""
    try:
        if value is None or value == "":
            return "0"
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"
