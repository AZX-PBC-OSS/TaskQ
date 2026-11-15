"""Typed deps-factory doors: valid products retain T; invalid call shapes red."""

from __future__ import annotations

from taskq.workflows import (
    WorkflowApp,
    WorkflowDepsContext,
    WorkflowDepsFactory,
    WorkflowDepsFactoryResult,
)


class Product:
    pass


def valid_factory(context: WorkflowDepsContext) -> Product:
    return Product()


def declared_factory_contract(context: WorkflowDepsContext) -> None:
    app = WorkflowApp(deps_factory=valid_factory)
    factory: WorkflowDepsFactory[Product] = valid_factory
    _ = app
    result: WorkflowDepsFactoryResult[Product] = factory(context)
    product = result
    _ = product


def missing_context_factory() -> Product:
    return Product()


WorkflowApp(
    deps_factory=missing_context_factory
)  # MUST_ERROR(reportArgumentType, invalid-argument-type): factory receives the actual construction context


def wrong_product_factory(context: WorkflowDepsContext) -> str:
    return str(context.generation)


wrong: WorkflowDepsFactory[Product] = (
    wrong_product_factory  # MUST_ERROR(reportAssignmentType, invalid-assignment): the declared product is Part of the public generic contract
)
