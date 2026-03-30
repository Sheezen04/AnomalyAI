from django import template

register = template.Library()


@register.filter
def dict_get(d, key):
    """Get a value from a dict by key in a Django template."""
    if isinstance(d, dict):
        val = d.get(key, '')
        if val is None:
            return ''
        if isinstance(val, float):
            return round(val, 4)
        return val
    return ''
