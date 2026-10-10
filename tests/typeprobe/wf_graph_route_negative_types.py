"""NEGATIVE TYPE PROBES — the graph-DSL typed route (T27): the route's
arms are keyed by the source's union MEMBERS' TYPES.

The graph-level face of the type-tagged Route's discipline (the chain
corpus's ``wf_type_tagged_route_negative_types.py``, at the wiring verbs):
a raw STRING key on a route dict — neither a union member nor any type —
reds at the call site; the declaration-time totality refusal (the verb's
own door) would refuse it too. The green face: a route keyed by the
union's member types passes UNMARKED (the type IS the tag; the arms'
placement rides ``RouteArm``).

Probes:

* ``probe_string_key_on_route_refused`` — a string key where the union
  member TYPE belongs: ``reportArgumentType`` (pyright) /
  ``invalid-argument-type`` (ty).
"""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows import Promise, RouteArm, StepContext, WorkflowApp, build, route, step


class ImageItem(BaseModel):
    doc_id: str


class AudioItem(BaseModel):
    doc_id: str


class ImageResult(BaseModel):
    doc_id: str


class AudioResult(BaseModel):
    doc_id: str


async def media_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    return [ImageItem(doc_id="d1"), AudioItem(doc_id="d2")]


async def process_image(ctx: object, item: ImageItem) -> ImageResult:
    return ImageResult(doc_id=item.doc_id)


async def process_audio(ctx: object, item: AudioItem) -> AudioResult:
    return AudioResult(doc_id=item.doc_id)


def probe_string_key_on_route_refused(app: WorkflowApp) -> None:
    """A STRING key on a route dict — the key is neither a union member
    nor any type: the checker refuses the literal (the type IS the tag)."""

    def build_fn() -> Promise[object]:
        src = step(media_source, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image),
                "AudioItem": RouteArm(
                    body=process_audio
                ),  # MUST_ERROR(reportArgumentType, invalid-argument-type): a str key
            },
        )
        return build(routed)

    build_fn()  # the probe's shape: the literal is the subject, the call keeps it live


def probe_the_green_face(app: WorkflowApp) -> None:
    """THE GREEN FACE (clean — never a marker): the arms keyed by the
    union's member TYPES, the promise the flat ``Promise[list[R]]`` with
    R solved to the union of the arms' returns."""

    def build_fn() -> Promise[object]:
        src = step(media_source, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image),
                AudioItem: RouteArm(body=process_audio),
            },
        )
        return build(routed)

    build_fn()


def probe_the_route_promise_type(app: WorkflowApp) -> None:
    """The route's promise carries the flat list of the arms' UNION (the
    typed sum's static face — the element type preserved, the gather's
    law at the route's join)."""

    def build_fn() -> Promise[list[ImageResult | AudioResult]]:
        src = step(media_source, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image),
                AudioItem: RouteArm(body=process_audio),
            },
        )
        return routed  # the promise IS the join's — the route's return IS the terminal

    build_fn()


class TextDoc(BaseModel):
    doc_id: str


class ImageDoc(BaseModel):
    doc_id: str


class ExtractedText(BaseModel):
    text: str


async def text_source(ctx: StepContext) -> list[TextDoc | ImageDoc]:
    return [TextDoc(doc_id="d1"), ImageDoc(doc_id="d2")]


async def extract_text(ctx: object, item: TextDoc) -> ExtractedText:
    return ExtractedText(text=item.doc_id)


async def ocr_text(ctx: object, item: ImageDoc) -> ExtractedText:
    return ExtractedText(text=item.doc_id)


def probe_the_homogeneous_barrier_type(app: WorkflowApp) -> None:
    """THE FAN-IN'S EDITOR FACE (T27's barrier — fork, fan back in at the
    chunking step): HOMOGENEOUS arms (both return ``ExtractedText``) —
    the route's promise solves to the flat ``Promise[list[ExtractedText]]``
    (the build fn's declared return pins the solve: a drifted ARM reds
    the RETURN — the list's invariance cannot smuggle the wrong element
    list past it, on pyright; ty's solving is looser here — the honest
    boundary — and the RUNTIME decode (the arm's param, the barrier's
    list param through the TypeAdapter) is the enforcement on every
    checker)."""

    def build_fn() -> Promise[list[ExtractedText]]:
        docs = step(text_source, key="docs")
        routed = route(
            docs,
            {
                TextDoc: RouteArm(body=extract_text),
                ImageDoc: RouteArm(body=ocr_text),
            },
        )
        return routed  # the solve's pin: R must BE ExtractedText exactly

    build_fn()
