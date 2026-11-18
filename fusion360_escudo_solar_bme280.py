import adsk.core
import adsk.fusion
import math

MM = 0.1


def value(distance_mm):
    return adsk.core.ValueInput.createByReal(distance_mm * MM)


def point(x, y, z=0.0):
    return adsk.core.Point3D.create(x * MM, y * MM, z * MM)


def transform_at_z(z):
    transform = adsk.core.Matrix3D.create()
    transform.translation = adsk.core.Vector3D.create(0.0, 0.0, z * MM)
    return transform


def offset_plane(component, base_plane, offset_mm):
    plane_input = component.constructionPlanes.createInput()
    plane_input.setByOffset(base_plane, value(offset_mm))
    return component.constructionPlanes.add(plane_input)


def xy_sketch(component, z=0.0):
    plane = offset_plane(component, component.xYConstructionPlane, z)
    return component.sketches.add(plane)


def xz_sketch(component, y=0.0):
    plane = offset_plane(component, component.xZConstructionPlane, y)
    return component.sketches.add(plane)


def yz_sketch(component, x=0.0):
    plane = offset_plane(component, component.yZConstructionPlane, x)
    return component.sketches.add(plane)


def add_polygon(sketch, coordinates):
    lines = sketch.sketchCurves.sketchLines
    for index, start in enumerate(coordinates):
        end = coordinates[(index + 1) % len(coordinates)]
        lines.addByTwoPoints(point(*start), point(*end))


def add_rectangle(sketch, x1, y1, x2, y2):
    return sketch.sketchCurves.sketchLines.addTwoPointRectangle(
        point(x1, y1), point(x2, y2)
    )


def add_circle(sketch, x, y, diameter):
    return sketch.sketchCurves.sketchCircles.addByCenterRadius(
        point(x, y), diameter * MM / 2.0
    )


def largest_profile(sketch):
    profiles = list(sketch.profiles)
    if not profiles:
        raise RuntimeError("El boceto no generó perfiles")
    return max(profiles, key=lambda profile: profile.areaProperties().area)


def annular_profile(sketch):
    for profile in sketch.profiles:
        if profile.profileLoops.count > 1:
            return profile
    raise RuntimeError("No se encontró el perfil anular")


def extrude(component, profile, distance_mm, operation, symmetric=False):
    feature_input = component.features.extrudeFeatures.createInput(profile, operation)
    if symmetric:
        feature_input.setSymmetricExtent(value(distance_mm), True)
    else:
        feature_input.setDistanceExtent(False, value(distance_mm))
    return component.features.extrudeFeatures.add(feature_input)


def revolve(component, sketch, operation=adsk.fusion.FeatureOperations.NewBodyFeatureOperation):
    feature_input = component.features.revolveFeatures.createInput(
        largest_profile(sketch),
        component.zConstructionAxis,
        operation,
    )
    feature_input.setAngleExtent(False, adsk.core.ValueInput.createByString("360 deg"))
    return component.features.revolveFeatures.add(feature_input)


def circular_pattern(component, features):
    feature_collection = adsk.core.ObjectCollection.create()
    for feature in features:
        feature_collection.add(feature)
    pattern_input = component.features.circularPatternFeatures.createInput(
        feature_collection, component.zConstructionAxis
    )
    pattern_input.quantity = adsk.core.ValueInput.createByString("3")
    pattern_input.totalAngle = adsk.core.ValueInput.createByString("360 deg")
    return component.features.circularPatternFeatures.add(pattern_input)


def build_louver(component):
    sketch = xz_sketch(component)
    add_polygon(
        sketch,
        [
            (21.0, 10.0),
            (25.0, 14.0),
            (45.0, -6.0),
            (43.0, -6.0),
            (25.0, 12.0),
            (22.6, 9.6),
        ],
    )
    return revolve(component, sketch)


def build_open_ring(component):
    build_louver(component)

    pillar_sketch = xy_sketch(component, 2.0)
    add_circle(pillar_sketch, 25.0, 0.0, 8.0)
    add_circle(pillar_sketch, 25.0, 0.0, 3.4)
    pillar = extrude(
        component,
        annular_profile(pillar_sketch),
        12.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
    )

    rib_sketch = xz_sketch(component)
    add_polygon(rib_sketch, [(25.0, 12.0), (29.0, 12.0), (29.0, 8.0)])
    rib = extrude(
        component,
        largest_profile(rib_sketch),
        2.4,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        symmetric=True,
    )
    circular_pattern(component, [pillar, rib])


def build_blind_ring(component):
    build_open_ring(component)

    bridge_sketch = xy_sketch(component, 10.0)
    add_rectangle(bridge_sketch, 16.0, -2.0, 27.0, 2.0)
    bridge = extrude(
        component,
        largest_profile(bridge_sketch),
        2.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
    )
    circular_pattern(component, [bridge])

    disk_sketch = xy_sketch(component, 10.0)
    add_circle(disk_sketch, 0.0, 0.0, 36.0)
    extrude(
        component,
        largest_profile(disk_sketch),
        2.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
    )


def build_top_cap(component):
    roof_sketch = xz_sketch(component)
    add_polygon(
        roof_sketch,
        [
            (0.0, 16.0),
            (25.0, 16.0),
            (45.0, -4.0),
            (43.0, -4.0),
            (24.2, 13.6),
            (0.0, 13.6),
        ],
    )
    revolve(component, roof_sketch)

    pillar_sketch = xy_sketch(component, 13.6)
    add_circle(pillar_sketch, 25.0, 0.0, 8.0)
    add_circle(pillar_sketch, 25.0, 0.0, 2.8)
    pillar = extrude(
        component,
        annular_profile(pillar_sketch),
        -11.6,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
    )
    circular_pattern(component, [pillar])


def add_capsule(sketch, center_x, center_y, length, width, vertical=False):
    radius = width / 2.0
    straight = length - width
    if vertical:
        add_rectangle(
            sketch,
            center_x - radius,
            center_y - straight / 2.0,
            center_x + radius,
            center_y + straight / 2.0,
        )
        add_circle(sketch, center_x, center_y - straight / 2.0, width)
        add_circle(sketch, center_x, center_y + straight / 2.0, width)
    else:
        add_rectangle(
            sketch,
            center_x - straight / 2.0,
            center_y - radius,
            center_x + straight / 2.0,
            center_y + radius,
        )
        add_circle(sketch, center_x - straight / 2.0, center_y, width)
        add_circle(sketch, center_x + straight / 2.0, center_y, width)


def add_sensor_guides(component):
    guide_sketch = xy_sketch(component, 6.0)
    left = [
        (-8.15, -0.925),
        (-6.15, -0.925),
        (-6.15, 1.075),
        (-4.95, 1.075),
        (-4.95, 2.925),
        (-6.15, 2.925),
        (-6.15, 4.925),
        (-8.15, 4.925),
    ]
    right = [(-x, y) for x, y in reversed(left)]
    add_polygon(guide_sketch, left)
    add_polygon(guide_sketch, right)
    profiles = sorted(
        list(guide_sketch.profiles), key=lambda profile: profile.areaProperties().area
    )
    if len(profiles) != 2:
        raise RuntimeError("Las guías del sensor no generaron dos perfiles")
    for profile in profiles:
        extrude(
            component,
            profile,
            28.0,
            adsk.fusion.FeatureOperations.JoinFeatureOperation,
        )

    for x1, x2 in [(-6.15, -4.95), (4.95, 6.15)]:
        stop_sketch = xy_sketch(component, 17.0)
        add_rectangle(stop_sketch, x1, 1.075, x2, 2.925)
        extrude(
            component,
            largest_profile(stop_sketch),
            1.0,
            adsk.fusion.FeatureOperations.JoinFeatureOperation,
        )

    for x in (-5.55, 5.55):
        clip_sketch = yz_sketch(component, x)
        add_polygon(clip_sketch, [(1.075, 34.3), (1.875, 33.5), (1.075, 33.5)])
        extrude(
            component,
            largest_profile(clip_sketch),
            2.0,
            adsk.fusion.FeatureOperations.JoinFeatureOperation,
            symmetric=True,
        )


def add_mounting_arm(component):
    arm_sketch = xy_sketch(component, 2.0)
    add_rectangle(arm_sketch, -12.0, 20.0, 12.0, 70.0)
    extrude(
        component,
        largest_profile(arm_sketch),
        6.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
    )

    for x in (-10.0, 10.0):
        rib_sketch = yz_sketch(component, x)
        add_polygon(rib_sketch, [(35.0, 2.0), (55.0, 2.0), (55.0, 12.0)])
        extrude(
            component,
            largest_profile(rib_sketch),
            2.0,
            adsk.fusion.FeatureOperations.JoinFeatureOperation,
            symmetric=True,
        )

    plate_sketch = xz_sketch(component, 65.0)
    add_rectangle(plate_sketch, -15.0, 2.0, 15.0, 32.0)
    extrude(
        component,
        largest_profile(plate_sketch),
        4.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        symmetric=True,
    )

    for x in (-8.0, 8.0):
        hole_sketch = xz_sketch(component, 65.0)
        add_circle(hole_sketch, x, 25.0, 4.5)
        extrude(
            component,
            largest_profile(hole_sketch),
            10.0,
            adsk.fusion.FeatureOperations.CutFeatureOperation,
            symmetric=True,
        )

        slot_sketch = xz_sketch(component, 65.0)
        add_capsule(slot_sketch, x, 10.0, 12.0, 3.0, vertical=True)
        extrude(
            component,
            largest_profile(slot_sketch),
            10.0,
            adsk.fusion.FeatureOperations.CutFeatureOperation,
            symmetric=True,
        )


def build_base(component):
    build_open_ring(component)

    ring_sketch = xy_sketch(component, 2.0)
    add_circle(ring_sketch, 0.0, 0.0, 58.0)
    add_circle(ring_sketch, 0.0, 0.0, 36.0)
    extrude(
        component,
        annular_profile(ring_sketch),
        4.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
    )

    hole_sketch = xy_sketch(component, 2.0)
    add_circle(hole_sketch, 25.0, 0.0, 3.4)
    through_hole = extrude(
        component,
        largest_profile(hole_sketch),
        4.0,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
    )

    recess_sketch = xy_sketch(component, 2.0)
    add_circle(recess_sketch, 25.0, 0.0, 6.5)
    recess = extrude(
        component,
        largest_profile(recess_sketch),
        3.0,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
    )
    circular_pattern(component, [through_hole, recess])

    bridge_sketch = xy_sketch(component, 2.0)
    add_rectangle(bridge_sketch, -22.0, -8.0, 22.0, 8.0)
    extrude(
        component,
        largest_profile(bridge_sketch),
        4.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
    )

    cable_sketch = xy_sketch(component, 2.0)
    add_capsule(cable_sketch, 0.0, -4.0, 10.0, 6.0)
    extrude(
        component,
        largest_profile(cable_sketch),
        4.0,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
    )

    add_sensor_guides(component)
    add_mounting_arm(component)


def add_parameters(design):
    parameters = [
        ("D_ext", "90 mm", "Diámetro exterior de los platos"),
        ("D_int", "42 mm", "Diámetro interior de la cámara"),
        ("D_pitch", "50 mm", "Diámetro del círculo de pilares"),
        ("P_z", "12 mm", "Paso vertical entre platos"),
        ("PCB_L", "15.5 mm", "Largo de la placa BME280"),
        ("PCB_W", "12 mm", "Ancho de la placa BME280"),
        ("PCB_T", "1.6 mm", "Espesor nominal de la placa"),
        ("PCB_slot", "1.85 mm", "Ancho de ranura con holgura ABS"),
        ("PCB_guides", "12.3 mm", "Separación de fondos de ranura"),
        ("M3_clearance", "3.4 mm", "Paso libre M3 para ABS"),
        ("M3_tap", "2.8 mm", "Piloto para rosca M3 en ABS"),
    ]
    for name, expression, comment in parameters:
        design.userParameters.add(
            name, adsk.core.ValueInput.createByString(expression), "mm", comment
        )


def hide_construction_geometry(design):
    for component in design.allComponents:
        for plane in component.constructionPlanes:
            plane.isLightBulbOn = False
        for sketch in component.sketches:
            sketch.isVisible = False


def run(_context: str):
    app = adsk.core.Application.get()
    design = adsk.fusion.Design.cast(app.activeProduct)
    if not design:
        raise RuntimeError("No hay un diseño Fusion activo")
    root = design.rootComponent
    if root.occurrences.count or root.bRepBodies.count:
        raise RuntimeError("El documento activo ya no está vacío; no se modificará")

    if design.unitsManager.defaultLengthUnits != "mm":
        raise RuntimeError("El documento activo no está configurado en milímetros")
    add_parameters(design)

    base_occurrence = root.occurrences.addNewComponent(transform_at_z(0.0))
    base_occurrence.component.name = "Comp_1_Base_Mount"
    base_occurrence.name = "Comp_1_Base_Mount"
    base_occurrence.isGrounded = True
    build_base(base_occurrence.component)

    open_occurrence = root.occurrences.addNewComponent(transform_at_z(12.0))
    open_component = open_occurrence.component
    open_component.name = "Comp_2_Open_Ring"
    open_occurrence.name = "Comp_2_Open_Ring_1"
    build_open_ring(open_component)

    for index, z in [(2, 24.0), (3, 36.0)]:
        occurrence = root.occurrences.addExistingComponent(
            open_component, transform_at_z(z)
        )
        occurrence.name = "Comp_2_Open_Ring_{}".format(index)

    blind_occurrence = root.occurrences.addNewComponent(transform_at_z(48.0))
    blind_occurrence.component.name = "Comp_3_Blind_Ring"
    blind_occurrence.name = "Comp_3_Blind_Ring"
    build_blind_ring(blind_occurrence.component)

    top_occurrence = root.occurrences.addNewComponent(transform_at_z(60.0))
    top_occurrence.component.name = "Comp_4_Top_Cap"
    top_occurrence.name = "Comp_4_Top_Cap"
    build_top_cap(top_occurrence.component)

    hide_construction_geometry(design)
    design.activateRootComponent()
    app.activeViewport.fit()

    for component in design.allComponents:
        print(
            "{}: {} cuerpo(s), {} boceto(s)".format(
                component.name, component.bRepBodies.count, component.sketches.count
            )
        )
    print("Ensamblaje creado con {} ocurrencias".format(root.occurrences.count))
