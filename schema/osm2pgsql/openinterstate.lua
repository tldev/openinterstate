-- OpenInterstate flex mapping for canonical OSM storage.
--
-- This mapping is intentionally narrow and deterministic:
-- - motorway_junction nodes for exits
-- - selected POI categories from node/way features
-- - routable highway ways for corridor/access derivations

local exits = osm2pgsql.define_node_table("osm2pgsql_v2_exits_nodes", {
    { column = "ref", type = "text" },
    { column = "name", type = "text" },
    { column = "tags", type = "jsonb" },
    { column = "geom", type = "point", projection = 4326, not_null = true },
}, { ids = { type = "node", id_column = "osm_id" } })

local poi_nodes = osm2pgsql.define_node_table("osm2pgsql_v2_poi_nodes", {
    { column = "category", type = "text", not_null = true },
    { column = "name", type = "text" },
    { column = "display_name", type = "text" },
    { column = "brand", type = "text" },
    { column = "tags", type = "jsonb" },
    { column = "geom", type = "point", projection = 4326, not_null = true },
}, { ids = { type = "node", id_column = "osm_id" } })

local poi_ways = osm2pgsql.define_table({
    name = "osm2pgsql_v2_poi_ways",
    ids = { type = "way", id_column = "osm_id" },
    columns = {
        { column = "category", type = "text", not_null = true },
        { column = "name", type = "text" },
        { column = "display_name", type = "text" },
        { column = "brand", type = "text" },
        { column = "tags", type = "jsonb" },
        { column = "geom", type = "geometry", projection = 4326, not_null = true },
    },
})

local highways = osm2pgsql.define_way_table("osm2pgsql_v2_highways", {
    { column = "highway", type = "text", not_null = true },
    { column = "ref", type = "text" },
    { column = "name", type = "text" },
    { column = "oneway", type = "text" },
    { column = "node_ids", sql_type = "int8[]" },
    { column = "tags", type = "jsonb" },
    { column = "geom", type = "linestring", projection = 4326, not_null = true },
}, { ids = { type = "way", id_column = "osm_id" } })

local function classify_poi(tags)
    local amenity = tags.amenity or ""
    local tourism = tags.tourism or ""
    local highway = tags.highway or ""
    local shop = tags.shop or ""

    if amenity == "fuel" or shop == "gas" then
        return "gas"
    end
    if tourism == "hotel" or tourism == "motel" or tourism == "guest_house" then
        return "lodging"
    end
    if amenity == "restaurant" or amenity == "fast_food" or amenity == "cafe" or tags.cuisine ~= nil then
        return "food"
    end
    if highway == "rest_area" or highway == "services" then
        return "restArea"
    end
    if amenity == "toilets" then
        return "restroom"
    end
    if amenity == "charging_station" then
        return "evCharging"
    end
    return nil
end

local function is_highway_class(v)
    return v == "motorway"
        or v == "motorway_link"
        or v == "trunk"
        or v == "trunk_link"
end

local function has_interstate_ref(tags)
    local ref = nil
    if tags.ref ~= nil and tags.ref ~= "" then
        ref = tags.ref
    elseif tags.int_ref ~= nil and tags.int_ref ~= "" then
        ref = tags.int_ref
    else
        ref = ""
    end

    for candidate in string.gmatch(ref, "([^;]+)") do
        local trimmed = candidate:gsub("^%s+", ""):gsub("%s+$", "")
        if string.match(trimmed, "^[Ii][%s%-]*%d+%a?$") ~= nil then
            return true
        end
    end
    return false
end

local function effective_highway_class(tags)
    local highway = tags.highway
    if is_highway_class(highway) then
        return highway
    end
    if highway == "construction" and is_highway_class(tags.construction) then
        return tags.construction
    end
    if highway == "construction" and has_interstate_ref(tags) then
        return "motorway"
    end
    return nil
end

function osm2pgsql.process_node(object)
    local tags = object.tags

    if tags.highway == "motorway_junction" then
        exits:insert({
            ref = tags.ref or tags["junction:ref"],
            name = tags.name or tags["exit:name"] or tags.destination or tags["destination:ref"],
            tags = tags,
            geom = object:as_point(),
        })
    end

    local cat = classify_poi(tags)
    if cat ~= nil then
        -- operator fallback excluded: operator tags (e.g. "Florida Department
        -- of Transportation") are not user-facing names.  Raw tags are stored
        -- in the tags jsonb column for derive-time access if needed.
        local display_name = tags.brand or tags.name
        poi_nodes:insert({
            category = cat,
            name = tags.name or tags.brand,
            display_name = display_name,
            brand = tags.brand,
            tags = tags,
            geom = object:as_point(),
        })
    end
end

function osm2pgsql.process_way(object)
    local tags = object.tags
    local hwy = effective_highway_class(tags)

    if hwy ~= nil then
        local geom = object:as_linestring()
        if geom ~= nil then
            -- Format node IDs as PostgreSQL int8[] literal: {id1,id2,...}
            local nodes = object.nodes
            local node_str = "{"
            for i, nid in ipairs(nodes) do
                if i > 1 then node_str = node_str .. "," end
                node_str = node_str .. tostring(nid)
            end
            node_str = node_str .. "}"

            highways:insert({
                highway = hwy,
                ref = tags.ref,
                name = tags.name,
                oneway = tags.oneway,
                node_ids = node_str,
                tags = tags,
                geom = geom,
            })
        end
    end

    local cat = classify_poi(tags)
    if cat ~= nil then
        local way_geom = object:as_polygon()
        if way_geom == nil then
            way_geom = object:as_linestring()
        end
        if way_geom ~= nil then
            local display_name = tags.brand or tags.name
            poi_ways:insert({
                category = cat,
                name = tags.name or tags.brand,
                display_name = display_name,
                brand = tags.brand,
                tags = tags,
                geom = way_geom,
            })
        end
    end
end
