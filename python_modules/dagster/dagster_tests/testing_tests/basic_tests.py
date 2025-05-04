from dataclasses import dataclass
from pathlib import Path

from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.decorators.asset_decorator import asset
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.resource_annotation import ResourceParam
from dagster._core.execution.context.invocation import build_asset_context
from dagster.components.component.component import Component
from dagster.components.core.context import ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model
from dagster.components.testing import component_asset, defs_from_component_yaml_path


def test_basic_test() -> None:
    class SimpleComponent(Component):
        def build_defs(self, context: ComponentLoadContext) -> Definitions:
            @asset
            def an_asset() -> int:
                return 1

            return Definitions([an_asset])

    an_asset = component_asset(component=SimpleComponent(), asset_key="an_asset")
    assert an_asset.key == AssetKey("an_asset")
    assert an_asset() == 1


def test_asset_with_resources() -> None:
    @dataclass
    class AResource:
        value: str

    class ComponentWithUnfulfilledResource(Component):
        def build_defs(self, context: ComponentLoadContext) -> Definitions:
            @asset
            def asset_with_resource(a_resource: ResourceParam[AResource]) -> str:
                return a_resource.value

            return Definitions([asset_with_resource])

    an_asset = component_asset(
        component=ComponentWithUnfulfilledResource(),
        asset_key="asset_with_resource",
        resources={"a_resource": AResource(value="foo")},
    )

    assert isinstance(an_asset, AssetsDefinition)
    assert an_asset() == "foo"


def test_components_with_declaration():
    class ComponentWithDeclaration(Component, Model, Resolvable):
        value: str

        def build_defs(self, context: ComponentLoadContext) -> Definitions:
            @asset
            def an_asset() -> str:
                return self.value

            return Definitions([an_asset])

    assert (
        component_asset(component=ComponentWithDeclaration(value="bar"), asset_key="an_asset")()
        == "bar"
    )

    assert (
        component_asset(
            component=ComponentWithDeclaration.from_dict(attributes={"value": "foobar"}),
            asset_key="an_asset",
        )()
        == "foobar"
    )


def test_component_on_disk():
    path = Path(__file__).parent / "some_value_simple_asset_component.yaml"
    assets_def = defs_from_component_yaml_path(component_yaml=path).get_assets_def("an_asset")
    assert assets_def(context=build_asset_context()) == "some_value"
