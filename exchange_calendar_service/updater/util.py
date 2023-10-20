import logging
from typing import Any

from typing_extensions import Dict, Callable

# Set up logging.
log = logging.getLogger(__name__)


def discover_entrypoints(group: str) -> Callable[[], Dict[str, Any]]:
    def discover() -> Dict[str, Callable[[], Dict[str, Any]]]:
        """
        Discover all entrypoints for the given group.

        Returns:
            A dictionary with the entry point name as key and the corresponding object as value.
        """
        try:
            import importlib.metadata as metadata
            entry_points = metadata.entry_points(group)
        except ImportError:
            import pkg_resources
            entry_points = pkg_resources.iter_entry_points(group)

        if entry_points is None:
            return {}

        result = {}

        for entry_point in entry_points:
            try:
                result[entry_point.name] = entry_point.load()
            except Exception as e:
                log.warning(f"Failed to load entry point with name {entry_point.name} from group {group}: {e}", exc_info=True)
                pass

        return result

    return discover
