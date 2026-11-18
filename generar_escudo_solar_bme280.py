import math
import os
import adsk.core
import adsk.fusion


def mm(val: float) -> float:
    """Convert millimeters to Fusion 360 internal units (centimeters)."""
    return val * 0.1


def sk_pt(sketch: adsk.fusion.Sketch, x_mm: float, y_mm: float, z_mm: float) -> adsk.core.Point3D:
    """Map a 3D point in component coordinates (mm) to 2D sketch space (cm)."""
    pt3d = adsk.core.Point3D.create(mm(x_mm), mm(y_mm), mm(z_mm))
    return sketch.modelToSketchSpace(pt3d)


def draw_closed_polygon_3d(sketch: adsk.fusion.Sketch, pts_3d_mm: list):
    """Draw a closed polygon on a sketch given a list of (x_mm, y_mm, z_mm) tuples."""
    s_pts = [sketch.sketchPoints.add(sk_pt(sketch, x, y, z)) for (x, y, z) in pts_3d_mm]
    lines = sketch.sketchCurves.sketchLines
    n = len(s_pts)
    for i in range(n):
        lines.addByTwoPoints(s_pts[i], s_pts[(i + 1) % n])


def draw_hexagon_xy(
    sketch: adsk.fusion.Sketch,
    cx_mm: float,
    cy_mm: float,
    z_mm: float,
    af_mm: float,
    rot_deg: float = 0.0,
):
    """
    Draw a regular hexagon centered at (cx_mm, cy_mm, z_mm) given its across-flats (AF) dimension.
    Circumradius R = AF / sqrt(3).
    """
    r_circum = af_mm / math.sqrt(3.0)
    pts = []
    for k in range(6):
        ang = math.radians(rot_deg + k * 60.0)
        pts.append((cx_mm + r_circum * math.cos(ang), cy_mm + r_circum * math.sin(ang), z_mm))
    draw_closed_polygon_3d(sketch, pts)


def create_offset_xy_plane(comp: adsk.fusion.Component, z_mm: float) -> adsk.fusion.ConstructionPlane:
    planes = comp.constructionPlanes
    p_input = planes.createInput()
    p_input.setByOffset(comp.xYConstructionPlane, adsk.core.ValueInput.createByReal(mm(z_mm)))
    return planes.add(p_input)


def extrude_profiles(
    comp: adsk.fusion.Component,
    profs: adsk.core.ObjectCollection,
    dist_mm: float,
    operation: int,
    target_body: adsk.fusion.BRepBody = None,
    symmetric: bool = False,
) -> adsk.fusion.ExtrudeFeature:
    extrudes = comp.features.extrudeFeatures
    ext_input = extrudes.createInput(profs, operation)
    if symmetric:
        ext_input.setSymmetricExtent(adsk.core.ValueInput.createByReal(mm(dist_mm)), True)
    else:
        extent = adsk.fusion.DistanceExtentDefinition.create(adsk.core.ValueInput.createByReal(mm(dist_mm)))
        ext_input.setOneSideExtent(extent, adsk.fusion.ExtentDirections.PositiveExtentDirection)
    if target_body is not None and operation != adsk.fusion.FeatureOperations.NewBodyFeatureOperation:
        ext_input.participantBodies = [target_body]
    return extrudes.add(ext_input)


def extrude_all_profiles(
    comp: adsk.fusion.Component,
    sketch: adsk.fusion.Sketch,
    dist_mm: float,
    operation: int,
    target_body: adsk.fusion.BRepBody = None,
    symmetric: bool = False,
) -> adsk.fusion.ExtrudeFeature:
    profs = adsk.core.ObjectCollection.create()
    for i in range(sketch.profiles.count):
        profs.add(sketch.profiles.item(i))
    return extrude_profiles(comp, profs, dist_mm, operation, target_body, symmetric)


def add_xy_interlock_and_hex_hole(
    comp: adsk.fusion.Component,
    body: adsk.fusion.BRepBody,
    has_bottom_male_boss: bool,
    has_top_female_socket: bool,
    cut_hex_through: bool = True,
):
    """
    Add XY interlocking male/female features on the 3 pillars (at R = 25.0 mm, D_pilar = 12.0 mm)
    and cut the larger hexagonal through-hole (AF = 6.0 mm).
    - Bottom male spigot (Z = 0.3 to 2.0 mm, height 1.7 mm): D_male = 9.2 mm (r = 4.6 mm).
    - Top female socket (Z = 12.0 to 14.0 mm, depth 2.0 mm): D_female = 9.6 mm (r = 4.8 mm).
    - Hexagonal through-hole: AF = 6.0 mm (across flats).
    """
    if has_bottom_male_boss:
        plane_z03 = create_offset_xy_plane(comp, 0.3)
        sk_male = comp.sketches.add(plane_z03)
        sk_male.name = "Sk_XY_Male_Bosses"
        for angle_deg in (0.0, 120.0, 240.0):
            rad = math.radians(angle_deg)
            cx = 25.0 * math.cos(rad)
            cy = 25.0 * math.sin(rad)
            sk_male.sketchCurves.sketchCircles.addByCenterRadius(
                sk_pt(sk_male, cx, cy, 0.3), mm(4.6)  # D = 9.2 mm
            )
        extrude_all_profiles(
            comp,
            sk_male,
            1.7,  # Z = 0.3 to 2.0 mm
            adsk.fusion.FeatureOperations.JoinFeatureOperation,
            target_body=body,
        )

    if has_top_female_socket:
        plane_z12 = create_offset_xy_plane(comp, 12.0)
        sk_fem = comp.sketches.add(plane_z12)
        sk_fem.name = "Sk_XY_Female_Sockets"
        for angle_deg in (0.0, 120.0, 240.0):
            rad = math.radians(angle_deg)
            cx = 25.0 * math.cos(rad)
            cy = 25.0 * math.sin(rad)
            sk_fem.sketchCurves.sketchCircles.addByCenterRadius(
                sk_pt(sk_fem, cx, cy, 12.0), mm(4.8)  # D = 9.6 mm
            )
        extrude_all_profiles(
            comp,
            sk_fem,
            2.5,  # Cut from Z = 12.0 up through Z = 14.0 mm
            adsk.fusion.FeatureOperations.CutFeatureOperation,
            target_body=body,
        )

    if cut_hex_through:
        sk_hex = comp.sketches.add(comp.xYConstructionPlane)
        sk_hex.name = "Sk_Hex_Holes_AF6mm"
        for angle_deg in (0.0, 120.0, 240.0):
            rad = math.radians(angle_deg)
            cx = 25.0 * math.cos(rad)
            cy = 25.0 * math.sin(rad)
            draw_hexagon_xy(sk_hex, cx, cy, 0.0, af_mm=6.0, rot_deg=angle_deg)
        extrude_all_profiles(
            comp,
            sk_hex,
            16.0,
            adsk.fusion.FeatureOperations.CutFeatureOperation,
            target_body=body,
        )


def build_louver_skirt_and_pillars(
    comp: adsk.fusion.Component,
    body_name: str,
    has_bottom_male_boss: bool = True,
    has_top_female_socket: bool = True,
    cut_hex_through: bool = True,
) -> adsk.fusion.BRepBody:
    """
    Conical louver skirt (45 deg) + 3 reinforced spacer pillars (D = 12.0 mm) + ribs + XY interlock.
    """
    sk_cone = comp.sketches.add(comp.xZConstructionPlane)
    sk_cone.name = f"Sk_{body_name}_Cone"
    cone_pts = [
        (21.0, 0.0, 10.0),  # P1: Inner upper lip
        (25.0, 0.0, 14.0),  # P2: Upper cone apex
        (45.0, 0.0, -6.0),  # P3: Outer lower drip edge
        (43.0, 0.0, -6.0),  # P4: 2.0 mm horizontal wall thickness
        (25.0, 0.0, 12.0),  # P5: Inner lower vertex
        (22.6, 0.0, 9.6),   # P6: Inner lip closure
    ]
    draw_closed_polygon_3d(sk_cone, cone_pts)

    revolves = comp.features.revolveFeatures
    rev_input = revolves.createInput(
        sk_cone.profiles.item(0),
        comp.zConstructionAxis,
        adsk.fusion.FeatureOperations.NewBodyFeatureOperation,
    )
    rev_input.setAngleExtent(False, adsk.core.ValueInput.createByString("360 deg"))
    rev_feat = revolves.add(rev_input)
    body = rev_feat.bodies.item(0)
    body.name = body_name

    # 3 Reinforced Standoff Pillars (D_pilar = 12.0 mm, r = 6.0 mm at R = 25.0 mm, Z = 2.0 to 14.0 mm)
    plane_z2 = create_offset_xy_plane(comp, 2.0)
    sk_pillars = comp.sketches.add(plane_z2)
    sk_pillars.name = f"Sk_{body_name}_Pillars_D12"
    circles = sk_pillars.sketchCurves.sketchCircles
    for angle_deg in (0.0, 120.0, 240.0):
        rad = math.radians(angle_deg)
        cx = 25.0 * math.cos(rad)
        cy = 25.0 * math.sin(rad)
        circles.addByCenterRadius(sk_pt(sk_pillars, cx, cy, 2.0), mm(6.0))

    extrude_all_profiles(
        comp,
        sk_pillars,
        12.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    sk_rib = comp.sketches.add(comp.xZConstructionPlane)
    sk_rib.name = f"Sk_{body_name}_Rib"
    rib_pts = [
        (25.0, 0.0, 12.0),
        (31.0, 0.0, 12.0),
        (31.0, 0.0, 6.0),
    ]
    draw_closed_polygon_3d(sk_rib, rib_pts)
    rib_feat = extrude_all_profiles(
        comp,
        sk_rib,
        3.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
        symmetric=True,
    )

    pat_entities = adsk.core.ObjectCollection.create()
    pat_entities.add(rib_feat)
    circ_pats = comp.features.circularPatternFeatures
    pat_input = circ_pats.createInput(pat_entities, comp.zConstructionAxis)
    pat_input.quantity = adsk.core.ValueInput.createByReal(3)
    pat_input.totalAngle = adsk.core.ValueInput.createByString("360 deg")
    pat_input.isSymmetric = False
    circ_pats.add(pat_input)

    if has_bottom_male_boss or has_top_female_socket or cut_hex_through:
        add_xy_interlock_and_hex_hole(
            comp,
            body,
            has_bottom_male_boss=has_bottom_male_boss,
            has_top_female_socket=has_top_female_socket,
            cut_hex_through=cut_hex_through,
        )

    return body


def build_comp2_open_ring(comp: adsk.fusion.Component) -> adsk.fusion.BRepBody:
    """COMPONENT 1 in spec: Comp_2_Open_Ring (Standard Ventilated Ring with XY interlock & hex hole)."""
    return build_louver_skirt_and_pillars(
        comp,
        "Body_Open_Ring",
        has_bottom_male_boss=True,
        has_top_female_socket=True,
        cut_hex_through=True,
    )


def build_comp4_top_cap(comp: adsk.fusion.Component) -> adsk.fusion.BRepBody:
    """
    COMPONENT 2 in spec: Comp_4_Top_Cap (Blind Upper Cap with XY male spigot and internal snap-lock pocket).
    """
    sk_roof = comp.sketches.add(comp.xZConstructionPlane)
    sk_roof.name = "Sk_Top_Cap_Roof"
    roof_pts = [
        (0.0, 0.0, 16.0),
        (25.0, 0.0, 16.0),
        (45.0, 0.0, -4.0),
        (43.0, 0.0, -4.0),
        (24.2, 0.0, 13.6),
        (0.0, 0.0, 13.6),
    ]
    draw_closed_polygon_3d(sk_roof, roof_pts)

    revolves = comp.features.revolveFeatures
    rev_input = revolves.createInput(
        sk_roof.profiles.item(0),
        comp.zConstructionAxis,
        adsk.fusion.FeatureOperations.NewBodyFeatureOperation,
    )
    rev_input.setAngleExtent(False, adsk.core.ValueInput.createByString("360 deg"))
    rev_feat = revolves.add(rev_input)
    body = rev_feat.bodies.item(0)
    body.name = "Body_Top_Cap"

    # 3 lower pillars (D = 12.0 mm, Z = 2.0 to 13.6 mm)
    plane_z2 = create_offset_xy_plane(comp, 2.0)
    sk_pillars = comp.sketches.add(plane_z2)
    sk_pillars.name = "Sk_Top_Cap_Pillars_D12"
    circles = sk_pillars.sketchCurves.sketchCircles
    for angle_deg in (0.0, 120.0, 240.0):
        rad = math.radians(angle_deg)
        cx = 25.0 * math.cos(rad)
        cy = 25.0 * math.sin(rad)
        circles.addByCenterRadius(sk_pt(sk_pillars, cx, cy, 2.0), mm(6.0))

    extrude_all_profiles(
        comp,
        sk_pillars,
        11.6,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Add the 3 bottom XY male spigots (D = 9.2 mm from Z = 0.3 to 2.0 mm) to interlock with Comp_3_Blind_Ring
    add_xy_interlock_and_hex_hole(
        comp,
        body,
        has_bottom_male_boss=True,
        has_top_female_socket=False,
        cut_hex_through=False,
    )

    # Trim any outer edge of the pillars that would protrude past the outer 45-deg roof cone
    sk_trim = comp.sketches.add(comp.xZConstructionPlane)
    sk_trim.name = "Sk_Top_Cap_Outer_Trim"
    trim_pts = [
        (25.0, 0.0, 16.0),
        (45.0, 0.0, 16.0),
        (45.0, 0.0, -4.0),
    ]
    draw_closed_polygon_3d(sk_trim, trim_pts)
    rev_trim_in = revolves.createInput(
        sk_trim.profiles.item(0),
        comp.zConstructionAxis,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
    )
    rev_trim_in.setAngleExtent(False, adsk.core.ValueInput.createByString("360 deg"))
    rev_trim_in.participantBodies = [body]
    revolves.add(rev_trim_in)

    # Cut Hexagonal shaft hole (AF = 6.0 mm) from Z = 0.0 up to Z = 9.2 mm (100% blind under the roof!)
    sk_hex = comp.sketches.add(comp.xYConstructionPlane)
    sk_hex.name = "Sk_Top_Cap_Hex_Hole"
    for angle_deg in (0.0, 120.0, 240.0):
        rad = math.radians(angle_deg)
        cx = 25.0 * math.cos(rad)
        cy = 25.0 * math.sin(rad)
        draw_hexagon_xy(sk_hex, cx, cy, 0.0, af_mm=6.0, rot_deg=angle_deg)
    extrude_all_profiles(
        comp,
        sk_hex,
        9.2,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
        target_body=body,
    )

    # Cut internal snap-lock retention chamber (Z = 5.5 to 9.2 mm, D = 9.0 mm)
    plane_z55 = create_offset_xy_plane(comp, 5.5)
    sk_snap_pocket = comp.sketches.add(plane_z55)
    sk_snap_pocket.name = "Sk_Top_Cap_Snap_Pocket"
    for angle_deg in (0.0, 120.0, 240.0):
        rad = math.radians(angle_deg)
        cx = 25.0 * math.cos(rad)
        cy = 25.0 * math.sin(rad)
        sk_snap_pocket.sketchCurves.sketchCircles.addByCenterRadius(
            sk_pt(sk_snap_pocket, cx, cy, 5.5), mm(4.5)
        )
    extrude_all_profiles(
        comp,
        sk_snap_pocket,
        3.7,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
        target_body=body,
    )
    return body


def build_comp3_blind_ring(comp: adsk.fusion.Component) -> adsk.fusion.BRepBody:
    """COMPONENT 3 in spec: Comp_3_Blind_Ring (Intermediate Thermal Deflector with XY interlock & hex hole)."""
    body = build_louver_skirt_and_pillars(
        comp,
        "Body_Blind_Ring",
        has_bottom_male_boss=False,
        has_top_female_socket=False,
        cut_hex_through=False,
    )

    plane_z10 = create_offset_xy_plane(comp, 10.0)

    # Extrude 3 radial bridges (width 5.0 mm, thickness 2.0 mm from Z=10 to Z=12 mm)
    sk_bridges = comp.sketches.add(plane_z10)
    sk_bridges.name = "Sk_Radial_Bridges"
    for angle_deg in (0.0, 120.0, 240.0):
        rad = math.radians(angle_deg)
        cos_a = math.cos(rad)
        sin_a = math.sin(rad)
        local_corners = [(16.0, -2.5), (25.0, -2.5), (25.0, 2.5), (16.0, 2.5)]
        pts_3d = [
            (r * cos_a - w * sin_a, r * sin_a + w * cos_a, 10.0)
            for (r, w) in local_corners
        ]
        draw_closed_polygon_3d(sk_bridges, pts_3d)

    extrude_all_profiles(
        comp,
        sk_bridges,
        2.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Extrude central baffle disk (D = 36.0 mm, r = 18.0 mm) at Z = 10.0 to 12.0 mm
    sk_disk = comp.sketches.add(plane_z10)
    sk_disk.name = "Sk_Deflector_Disk"
    sk_disk.sketchCurves.sketchCircles.addByCenterRadius(sk_pt(sk_disk, 0.0, 0.0, 10.0), mm(18.0))
    extrude_all_profiles(
        comp,
        sk_disk,
        2.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Add XY male/female interlock and cut the 3 hexagonal through-holes (AF = 6.0 mm)
    add_xy_interlock_and_hex_hole(
        comp,
        body,
        has_bottom_male_boss=True,
        has_top_female_socket=True,
        cut_hex_through=True,
    )
    return body


def build_comp1_base_mount(comp: adsk.fusion.Component) -> adsk.fusion.BRepBody:
    """COMPONENT 4 in spec: Comp_1_Base_Mount (Base, BME280 Holder Tower, and Mounting Arm)."""
    body = build_louver_skirt_and_pillars(
        comp,
        "Body_Base_Mount",
        has_bottom_male_boss=False,
        has_top_female_socket=False,
        cut_hex_through=False,
    )

    # Step 4.1.2: Structural ring on XY (Z = 2.0 to 6.0 mm), OD = 62.0 mm (R=31 to match D12 pillars), ID = 36.0 mm (R=18)
    plane_z2 = create_offset_xy_plane(comp, 2.0)
    sk_ring = comp.sketches.add(plane_z2)
    sk_ring.name = "Sk_Structural_Ring"
    c_ring = sk_ring.sketchCurves.sketchCircles
    c_ring.addByCenterRadius(sk_pt(sk_ring, 0.0, 0.0, 2.0), mm(31.0))
    c_ring.addByCenterRadius(sk_pt(sk_ring, 0.0, 0.0, 2.0), mm(18.0))

    ring_profs = adsk.core.ObjectCollection.create()
    for i in range(sk_ring.profiles.count):
        p = sk_ring.profiles.item(i)
        if p.profileLoops.count == 2:
            ring_profs.add(p)
    extrude_profiles(
        comp,
        ring_profs,
        4.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Step 4.1.3: Central diametral bridge (X = 44.0 mm, Y = 16.0 mm, Z = 2.0 to 6.0 mm)
    sk_bridge = comp.sketches.add(plane_z2)
    sk_bridge.name = "Sk_Central_Bridge"
    bridge_pts = [
        (-22.0, -8.0, 2.0),
        (22.0, -8.0, 2.0),
        (22.0, 8.0, 2.0),
        (-22.0, 8.0, 2.0),
    ]
    draw_closed_polygon_3d(sk_bridge, bridge_pts)
    extrude_all_profiles(
        comp,
        sk_bridge,
        4.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Step 4.1.3b: Oval cable slot (10.0 mm x 6.0 mm) centered at (X = 0, Y = -4.0 mm), cut Z = 2.0 to 6.0 mm
    sk_slot = comp.sketches.add(plane_z2)
    sk_slot.name = "Sk_Cable_Slot"
    slot_pts = []
    for k in range(9):
        ang = -math.pi / 2 + k * (math.pi / 8)
        slot_pts.append((2.0 + 3.0 * math.cos(ang), -4.0 + 3.0 * math.sin(ang), 2.0))
    for k in range(9):
        ang = math.pi / 2 + k * (math.pi / 8)
        slot_pts.append((-2.0 + 3.0 * math.cos(ang), -4.0 + 3.0 * math.sin(ang), 2.0))
    draw_closed_polygon_3d(sk_slot, slot_pts)
    extrude_all_profiles(
        comp,
        sk_slot,
        5.0,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
        target_body=body,
    )

    # Add top XY female sockets (Z = 12.0 to 14.0 mm, D = 9.6 mm) and cut Hexagonal through-holes (AF = 6.0 mm)
    add_xy_interlock_and_hex_hole(
        comp,
        body,
        has_bottom_male_boss=False,
        has_top_female_socket=True,
        cut_hex_through=True,
    )

    # Bottom counterbores (D = 9.8 mm x 3.0 mm deep from Z=2 to Z=5 mm) to recess the head of Comp_5_Hex_Pin
    sk_cbore = comp.sketches.add(plane_z2)
    sk_cbore.name = "Sk_Base_Pin_Head_Counterbores"
    for angle_deg in (0.0, 120.0, 240.0):
        rad = math.radians(angle_deg)
        cx = 25.0 * math.cos(rad)
        cy = 25.0 * math.sin(rad)
        sk_cbore.sketchCurves.sketchCircles.addByCenterRadius(sk_pt(sk_cbore, cx, cy, 2.0), mm(4.9))
    extrude_all_profiles(
        comp,
        sk_cbore,
        3.0,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
        target_body=body,
    )

    # Step 4.2: BME280 Holder Tower on Z = 6.0 mm up to Z = 34.0 mm (centered at X = 0, Y = 2.0 mm)
    plane_z6 = create_offset_xy_plane(comp, 6.0)
    sk_tower = comp.sketches.add(plane_z6)
    sk_tower.name = "Sk_BME280_U_Columns"

    left_u = [
        (-8.15, -0.925, 6.0),
        (-4.95, -0.925, 6.0),
        (-4.95, 1.075, 6.0),
        (-6.15, 1.075, 6.0),
        (-6.15, 2.925, 6.0),
        (-4.95, 2.925, 6.0),
        (-4.95, 4.925, 6.0),
        (-8.15, 4.925, 6.0),
    ]
    right_u = [(-x, y, z) for (x, y, z) in reversed(left_u)]
    draw_closed_polygon_3d(sk_tower, left_u)
    draw_closed_polygon_3d(sk_tower, right_u)
    extrude_all_profiles(
        comp,
        sk_tower,
        28.0,  # Z = 6.0 to 34.0 mm
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Step 4.2.4: Internal PCB height stop ledges (1.0 mm step inside both slots from Z = 6.0 to Z = 18.0 mm)
    sk_stops = comp.sketches.add(plane_z6)
    sk_stops.name = "Sk_PCB_Height_Stops"
    left_stop = [
        (-6.15, 1.075, 6.0),
        (-5.15, 1.075, 6.0),
        (-5.15, 2.925, 6.0),
        (-6.15, 2.925, 6.0),
    ]
    right_stop = [
        (5.15, 1.075, 6.0),
        (6.15, 1.075, 6.0),
        (6.15, 2.925, 6.0),
        (5.15, 2.925, 6.0),
    ]
    draw_closed_polygon_3d(sk_stops, left_stop)
    draw_closed_polygon_3d(sk_stops, right_stop)
    extrude_all_profiles(
        comp,
        sk_stops,
        12.0,  # Z = 6.0 to 18.0 mm
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Step 4.2.5: Cantilever snap-fit catches at top of columns (Z = 33.6 mm, 0.8 mm overhang, 45 deg ramp)
    planes = comp.constructionPlanes
    p_xz_in = planes.createInput()
    p_xz_in.setByOffset(comp.xZConstructionPlane, adsk.core.ValueInput.createByReal(mm(2.0)))
    plane_y2 = planes.add(p_xz_in)

    sk_snap = comp.sketches.add(plane_y2)
    sk_snap.name = "Sk_Snap_Locks"
    left_snap = [
        (-6.15, 2.0, 32.8),
        (-5.35, 2.0, 33.6),
        (-6.15, 2.0, 34.0),
    ]
    right_snap = [
        (6.15, 2.0, 32.8),
        (5.35, 2.0, 33.6),
        (6.15, 2.0, 34.0),
    ]
    draw_closed_polygon_3d(sk_snap, left_snap)
    draw_closed_polygon_3d(sk_snap, right_snap)
    extrude_all_profiles(
        comp,
        sk_snap,
        1.8,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
        symmetric=True,
    )

    # Step 4.3: Mast / Wall Mounting Arm extending in +Y from base ring to Y = 70.0 mm
    sk_arm = comp.sketches.add(plane_z2)
    sk_arm.name = "Sk_Mounting_Arm"
    arm_pts = [
        (-12.0, 22.0, 2.0),
        (12.0, 22.0, 2.0),
        (12.0, 70.0, 2.0),
        (-12.0, 70.0, 2.0),
    ]
    draw_closed_polygon_3d(sk_arm, arm_pts)
    extrude_all_profiles(
        comp,
        sk_arm,
        6.0,  # Z = 2.0 to 8.0 mm
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # 45-degree triangular ribs on arm up to Z = 12.0 mm
    for x_rib in (-10.5, 10.5):
        p_yz_in = planes.createInput()
        p_yz_in.setByOffset(comp.yZConstructionPlane, adsk.core.ValueInput.createByReal(mm(x_rib)))
        plane_x = planes.add(p_yz_in)
        sk_arib = comp.sketches.add(plane_x)
        arib_pts = [
            (x_rib, 35.0, 8.0),
            (x_rib, 66.0, 8.0),
            (x_rib, 66.0, 12.0),
            (x_rib, 39.0, 12.0),
        ]
        draw_closed_polygon_3d(sk_arib, arib_pts)
        extrude_all_profiles(
            comp,
            sk_arib,
            3.0,
            adsk.fusion.FeatureOperations.JoinFeatureOperation,
            target_body=body,
            symmetric=True,
        )

    # Vertical mounting plate at end of arm (Y = 66.0 to 70.0 mm, 30.0 mm x 30.0 mm: X in [-15, 15], Z in [-6, 24])
    p_plate_in = planes.createInput()
    p_plate_in.setByOffset(comp.xZConstructionPlane, adsk.core.ValueInput.createByReal(mm(66.0)))
    plane_y66 = planes.add(p_plate_in)

    sk_plate = comp.sketches.add(plane_y66)
    sk_plate.name = "Sk_Vertical_Plate_30x30"
    plate_pts = [
        (-15.0, 66.0, -6.0),
        (15.0, 66.0, -6.0),
        (15.0, 66.0, 24.0),
        (-15.0, 66.0, 24.0),
    ]
    draw_closed_polygon_3d(sk_plate, plate_pts)
    extrude_all_profiles(
        comp,
        sk_plate,
        4.0,  # Y = 66.0 to 70.0 mm
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    # Cut 2 wall screw holes (D = 4.5 mm) and 2 vertical strap slots (12.0 mm x 3.0 mm) through plate
    sk_plate_cuts = comp.sketches.add(plane_y66)
    sk_plate_cuts.name = "Sk_Plate_Holes_And_Slots"
    for x_h in (-10.0, 10.0):
        sk_plate_cuts.sketchCurves.sketchCircles.addByCenterRadius(
            sk_pt(sk_plate_cuts, x_h, 66.0, 19.0), mm(2.25)
        )
    for x_s in (-9.0, 9.0):
        s_pts = [
            (x_s - 1.5, 66.0, 0.0),
            (x_s + 1.5, 66.0, 0.0),
            (x_s + 1.5, 66.0, 12.0),
            (x_s - 1.5, 66.0, 12.0),
        ]
        draw_closed_polygon_3d(sk_plate_cuts, s_pts)

    extrude_all_profiles(
        comp,
        sk_plate_cuts,
        12.0,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
        target_body=body,
        symmetric=True,
    )
    return body


def build_comp5_hex_pin(comp: adsk.fusion.Component) -> adsk.fusion.BRepBody:
    """
    COMPONENT 5: Comp_5_Hex_Pin (Hexagonal Locking Pin designed to print flat on the bed).
    - Oriented along the Y axis (length = 69.5 mm) with its bottom flat resting on Z = 0.0 mm!
    - Cross-section on XZ plane: regular hexagon with flat top and flat bottom:
      * Shaft AF = 5.6 mm (Z from 0.0 to 5.6 mm, X from -3.23 to +3.23 mm), length Y = 0.0 to 66.5 mm.
      * Head at Y = -3.0 to 0.0 mm: hexagonal head with AF_Z = 5.6 mm (flush on bed Z = 0.0 to 5.6 mm)
        and wider X flanges (X from -4.5 to +4.5 mm).
      * Split Snap-Fit barbs at Y = 63.0 to 66.5 mm: flexible bifurcated barbs in X that flex in the XY
        bed plane and snap onto the Z = 8.0 mm internal ledge inside Comp_4_Top_Cap!
    """
    r_circum = 5.6 / math.sqrt(3.0)  # 3.233 mm
    half_side = r_circum * 0.5       # 1.617 mm

    sk_shaft = comp.sketches.add(comp.xZConstructionPlane)
    sk_shaft.name = "Sk_Hex_Pin_Shaft_Section"
    hex_xz_pts = [
        (-half_side, 0.0, 0.0),
        (half_side, 0.0, 0.0),
        (r_circum, 0.0, 2.8),
        (half_side, 0.0, 5.6),
        (-half_side, 0.0, 5.6),
        (-r_circum, 0.0, 2.8),
    ]
    draw_closed_polygon_3d(sk_shaft, hex_xz_pts)
    feat_shaft = extrude_all_profiles(
        comp,
        sk_shaft,
        64.0,
        adsk.fusion.FeatureOperations.NewBodyFeatureOperation,
    )
    body = feat_shaft.bodies.item(0)
    body.name = "Body_Hex_Pin"

    planes = comp.constructionPlanes
    p_head_in = planes.createInput()
    p_head_in.setByOffset(comp.xZConstructionPlane, adsk.core.ValueInput.createByReal(mm(-3.0)))
    plane_ym3 = planes.add(p_head_in)

    sk_head = comp.sketches.add(plane_ym3)
    sk_head.name = "Sk_Hex_Pin_Head"
    head_xz_pts = [
        (-2.9, -3.0, 0.0),
        (2.9, -3.0, 0.0),
        (4.5, -3.0, 2.8),
        (2.9, -3.0, 5.6),
        (-2.9, -3.0, 5.6),
        (-4.5, -3.0, 2.8),
    ]
    draw_closed_polygon_3d(sk_head, head_xz_pts)
    extrude_all_profiles(
        comp,
        sk_head,
        3.0,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    plane_z1 = create_offset_xy_plane(comp, 1.0)
    sk_barbs = comp.sketches.add(plane_z1)
    sk_barbs.name = "Sk_Snap_Barbs_XY"
    left_barb = [
        (-2.5, 60.5, 1.0),
        (-4.1, 60.5, 1.0),
        (-2.8, 64.0, 1.0),
        (-2.0, 64.0, 1.0),
    ]
    right_barb = [
        (2.5, 60.5, 1.0),
        (4.1, 60.5, 1.0),
        (2.8, 64.0, 1.0),
        (2.0, 64.0, 1.0),
    ]
    draw_closed_polygon_3d(sk_barbs, left_barb)
    draw_closed_polygon_3d(sk_barbs, right_barb)
    extrude_all_profiles(
        comp,
        sk_barbs,
        3.6,
        adsk.fusion.FeatureOperations.JoinFeatureOperation,
        target_body=body,
    )

    sk_flex_slot = comp.sketches.add(comp.xYConstructionPlane)
    sk_flex_slot.name = "Sk_Pin_Flex_Slot"
    slot_pts = [
        (-0.9, 48.0, 0.0),
        (0.9, 48.0, 0.0),
        (0.9, 64.5, 0.0),
        (-0.9, 64.5, 0.0),
    ]
    draw_closed_polygon_3d(sk_flex_slot, slot_pts)
    extrude_all_profiles(
        comp,
        sk_flex_slot,
        7.0,
        adsk.fusion.FeatureOperations.CutFeatureOperation,
        target_body=body,
    )
    return body


def run(_context: str):
    app = adsk.core.Application.get()
    design = adsk.fusion.Design.cast(app.activeProduct)
    design.designType = adsk.fusion.DesignTypes.ParametricDesignType
    design.designIntent = adsk.fusion.DesignIntentTypes.HybridDesignIntentType
    root = design.rootComponent

    while root.occurrences.count > 0:
        root.occurrences.item(0).deleteMe()

    def make_comp(name: str):
        occ = root.occurrences.addNewComponent(adsk.core.Matrix3D.create())
        occ.component.name = name
        return occ

    occ_base = make_comp("Comp_1_Base_Mount")
    build_comp1_base_mount(occ_base.component)

    occ_open1 = make_comp("Comp_2_Open_Ring")
    build_comp2_open_ring(occ_open1.component)

    occ_blind = make_comp("Comp_3_Blind_Ring")
    build_comp3_blind_ring(occ_blind.component)

    occ_top = make_comp("Comp_4_Top_Cap")
    build_comp4_top_cap(occ_top.component)

    occ_pin1 = make_comp("Comp_5_Hex_Pin")
    build_comp5_hex_pin(occ_pin1.component)

    occ_open2 = root.occurrences.addExistingComponent(occ_open1.component, adsk.core.Matrix3D.create())
    occ_open3 = root.occurrences.addExistingComponent(occ_open1.component, adsk.core.Matrix3D.create())

    occ_pin2 = root.occurrences.addExistingComponent(occ_pin1.component, adsk.core.Matrix3D.create())
    occ_pin3 = root.occurrences.addExistingComponent(occ_pin1.component, adsk.core.Matrix3D.create())

    def set_occ_xyz(occ, x_val, y_val, z_val):
        t = occ.transform2
        t.translation = adsk.core.Vector3D.create(mm(x_val), mm(y_val), mm(z_val))
        occ.transform2 = t

    set_occ_xyz(occ_base, 0.0, 0.0, 0.0)
    occ_base.isGrounded = True
    set_occ_xyz(occ_open1, 0.0, 0.0, 12.0)
    set_occ_xyz(occ_open2, 0.0, 0.0, 24.0)
    set_occ_xyz(occ_open3, 0.0, 0.0, 36.0)
    set_occ_xyz(occ_blind, 0.0, 0.0, 48.0)
    set_occ_xyz(occ_top, 0.0, 0.0, 60.0)

    set_occ_xyz(occ_pin1, 60.0, -30.0, -6.0)
    set_occ_xyz(occ_pin2, 72.0, -30.0, -6.0)
    set_occ_xyz(occ_pin3, 84.0, -30.0, -6.0)

    for c in design.allComponents:
        for i in range(c.constructionPlanes.count):
            c.constructionPlanes.item(i).isLightBulbOn = False
        for i in range(c.sketches.count):
            c.sketches.item(i).isLightBulbOn = False

    export_dir = r"c:\Users\Dairo\OneDrive\Documents\Proyectos\LiceoSN\davmobile\cad_exports"
    os.makedirs(export_dir, exist_ok=True)
    exp_mgr = design.exportManager

    for comp_name in (
        "Comp_1_Base_Mount",
        "Comp_2_Open_Ring",
        "Comp_3_Blind_Ring",
        "Comp_4_Top_Cap",
        "Comp_5_Hex_Pin",
    ):
        for c in design.allComponents:
            if c.name.startswith(comp_name):
                stl_path = os.path.join(export_dir, f"{comp_name}.stl")
                stl_opts = exp_mgr.createSTLExportOptions(c, stl_path)
                stl_opts.meshRefinement = adsk.fusion.MeshRefinementSettings.MeshRefinementHigh
                exp_mgr.execute(stl_opts)
                break

    asm_stl_path = os.path.join(export_dir, "Assembly_Escudo_Solar_BME280.stl")
    asm_opts = exp_mgr.createSTLExportOptions(root, asm_stl_path)
    asm_opts.meshRefinement = adsk.fusion.MeshRefinementSettings.MeshRefinementHigh
    exp_mgr.execute(asm_opts)

    app.activeViewport.fit()
