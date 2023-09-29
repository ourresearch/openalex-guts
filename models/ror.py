from app import db



class Ror(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "ror_summary"

    ror_id = db.Column(db.Text, db.ForeignKey("mid.institution.ror_id"), primary_key=True)
    name = db.Column(db.Text)
    city = db.Column(db.Text)
    state = db.Column(db.Text)
    country = db.Column(db.Text)
    country_code = db.Column(db.Text)
    grid_id = db.Column(db.Text)
    wikipedia_url = db.Column(db.Text)
    ror_type = db.Column(db.Text)

    @property
    def ror_url(self):
        return "https://ror.org/{}".format(self.ror_id)

    @property
    def country_code_upper(self):
        if not self.country_code:
            return None
        return self.country_code.upper()

    def __repr__(self):
        return "<Ror ( {} ) {}>".format(self.ror_id, self.name)


def ror_short_id(ror_id):
    return ror_id.replace("https://ror.org/", "")


class RorAcronyms(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_acronyms"

    ror_id = db.Column(db.Text, primary_key=True)
    acronym = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for acronym in item['acronyms']:
            yield cls(
                ror_id=ror_short_id(item['id']),
                acronym=acronym
            )


class RorAddresses(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_addresses"

    ror_id = db.Column(db.Text, primary_key=True)
    line_1 = db.Column(db.Text)
    line_2 = db.Column(db.Text)
    line_3 = db.Column(db.Text)
    lat = db.Column(db.Numeric)
    lng = db.Column(db.Numeric)
    postcode = db.Column(db.Text)
    is_primary = db.Column(db.Boolean)
    city = db.Column(db.Text, primary_key=True)
    state = db.Column(db.Text)
    state_code = db.Column(db.Text)
    country = db.Column(db.Text, primary_key=True)
    country_code = db.Column(db.Text, primary_key=True)
    geonames_city_id = db.Column(db.Text)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for i, address in enumerate(item['addresses']):
            if i == 0:
                is_primary = True
            else:
                is_primary = False
            yield cls(
                ror_id=ror_short_id(item['id']),
                line_1=address['line'],
                lat=address['lat'],
                lng=address['lng'],
                postcode=address['postcode'],
                is_primary=is_primary,
                city=address['city'],
                state=address['state'],
                state_code=address['state_code'],
                country=item['country']['country_name'],
                country_code=item['country']['country_code'],
                geonames_city_id=address['geonames_city']['id']
            )


class RorAliases(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_aliases"

    ror_id = db.Column(db.Text, primary_key=True)
    alias = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for alias in item['aliases']:
            yield cls(
                ror_id=ror_short_id(item['id']),
                alias=alias
            )


class RorBase(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_base"

    ror_id = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)
    city = db.Column(db.Text)
    state = db.Column(db.Text)
    country = db.Column(db.Text)

    @classmethod
    def from_ror_entry(cls, item):
        return cls(
            ror_id=ror_short_id(item["id"]),
            name=item['name'],
            city=item['addresses'][0]['city'],
            state=item['addresses'][0]['state'],
            country=item['country']['country_name'],
        )

class RorExternalIds(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_external_ids"

    ror_id = db.Column(db.Text, primary_key=True)
    external_id_type = db.Column(db.Text, primary_key=True)
    external_id = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for external_id_type, v in item['external_ids'].items():
            if external_id_type.upper() == 'GRID':
                continue
            for external_id in v['all']:
                yield cls(
                    ror_id=ror_short_id(item['id']),
                    external_id_type=external_id_type,
                    external_id=external_id
                )


class RorGridEquivalents(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_grid_equivalents"

    ror_id = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text, primary_key=True)
    country_code = db.Column(db.Text, primary_key=True)
    grid_id = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for external_id_type, v in item['external_ids'].items():
            if external_id_type.upper() == 'GRID':
                yield cls(
                    ror_id=ror_short_id(item['id']),
                    name=item['name'],
                    country_code=item['country']['country_code'],
                    grid_id=v['preferred']
                )


class RorInstitutes(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_institutes"

    ror_id = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)
    wikipedia_url = db.Column(db.Text)
    established = db.Column(db.Numeric)
    close = db.Column(db.Numeric)

    @classmethod
    def from_ror_entry(cls, item):
        return cls(
            ror_id=ror_short_id(item['id']),
            name=item['name'],
            wikipedia_url=item['wikipedia_url'],
            established=item['established']
        )


class RorLabels(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_labels"

    ror_id = db.Column(db.Text, primary_key=True)
    iso639 = db.Column(db.Text, primary_key=True)
    label = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for label_obj in item['labels']:
            yield cls(
                ror_id=ror_short_id(item['id']),
                iso639=label_obj['iso639'],
                label=label_obj['label']
            )


class RorLinks(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_links"

    ror_id = db.Column(db.Text, primary_key=True)
    link = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for link in item['links']:
            yield cls(
                ror_id=ror_short_id(item['id']),
                link=link
            )


class RorRelationships(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_relationships"

    ror_id = db.Column(db.Text, primary_key=True)
    relationship_type = db.Column(db.Text, primary_key=True)
    related_grid_id = db.Column(db.Text)
    related_ror_id = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for relationship in item['relationships']:
            yield cls(
                ror_id=ror_short_id(item['id']),
                relationship_type=relationship['type'],
                related_ror_id=ror_short_id(relationship['id'])
            )


class RorTypes(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_types"

    ror_id = db.Column(db.Text, primary_key=True)
    type = db.Column(db.Text, primary_key=True)

    @classmethod
    def yield_from_ror_entry(cls, item):
        for ror_type in item['types']:
            yield cls(
                ror_id=ror_short_id(item['id']),
                type=ror_type
            )


class RorUpdates(db.Model):
    __table_args__ = {"schema": "ins"}
    __tablename__ = "ror_updates"

    md5_checksum = db.Column(db.Text, primary_key=True)
    url = db.Column(db.Text)
    filename = db.Column(db.Text)
    size = db.Column(db.BigInteger)
    downloaded_at = db.Column(db.DateTime)
    finished_update_at = db.Column(db.DateTime)