"""Testing utilities for components."""

from pathlib import Path
from typing import Any, Optional

from dagster._core.definitions.asset_key import CoercibleToAssetKey
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.definitions_class import Definitions
from dagster.components.component.component import Component
from dagster.components.core.context import ComponentLoadContext
from dagster.components.core.defs_module import load_yaml_component_from_path


def component_asset(
    *,
    component: Component,
    asset_key: CoercibleToAssetKey,
    resources: Optional[dict[str, Any]] = None,
    context: Optional[ComponentLoadContext] = None,
) -> AssetsDefinition:
    """Get an asset from a component.

    Args:
        component: The component to get the asset from
        asset_key: The key of the asset to get
        resources: Optional resources to provide to the component
        context: Optional context to use when building the component

    Returns:
        The asset definition
    """
    defs = component_defs(component=component, resources=resources, context=context)
    return defs.get_assets_def(asset_key)


def component_defs(
    *,
    component: Component,
    resources: Optional[dict[str, Any]] = None,
    context: Optional[ComponentLoadContext] = None,
) -> Definitions:
    """Get definitions from a component.

    Args:
        component: The component to get definitions from
        resources: Optional resources to provide to the component
        context: Optional context to use when building the component

    Returns:
        The definitions
    """
    defs = component.build_defs(context or ComponentLoadContext.for_test())
    if resources:
        defs = Definitions.merge(defs, Definitions(resources=resources))
    return defs


def defs_from_component_yaml_path(
    *,
    component_yaml: Path,
    context: Optional[ComponentLoadContext] = None,
    resources: Optional[dict[str, Any]] = None,
):
    context = context or ComponentLoadContext.for_test()
    component = load_yaml_component_from_path(context=context, component_def_path=component_yaml)
    return component_defs(component=component, resources=resources, context=context)
