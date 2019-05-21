from typing import *
import utils


class Attributes(utils.Entity):
    name: str


class EntityBase(utils.Entity):
    type: str
    id: str
    attributes: Attributes
    links: Optional[Dict[str, utils.Entity]]


class CropAttributes(Attributes):
    slug: str
    binomial_name: Optional[str]
    common_names: Optional[List[str]]
    description: Optional[str]
    sun_requirements: Optional[str]
    sowing_method: Optional[str]
    spread: Optional[int]
    row_spacing: Optional[int]
    height: Optional[int]
    processing_pictures: int
    guides_count: int
    main_image_path: str
    taxon: Optional[str]
    tags_array: List[str]
    growing_degree_days: int
    svg_icon: Optional[str]


class CropRelationship(utils.Entity):
    links: Dict[str, utils.Entity]
    data: Optional[List[str]]


class Crop(EntityBase):
    attributes: CropAttributes
    relationships: Optional[Dict[str, CropRelationship]]
