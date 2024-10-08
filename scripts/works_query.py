from sqlalchemy import orm
from sqlalchemy.orm import selectinload

import models
from app import db


def base_works_query():
    return db.session.query(models.Work).options(
        selectinload(models.Work.records).selectinload
        (models.Record.journals).selectinload
        (models.Source.merged_into_source).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.journals).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.unpaywall).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.parseland_record).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.pdf_record).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.mag_record).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.legacy_records).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.hal_records).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.child_records).raiseload('*'),
        selectinload(models.Work.records).selectinload
        (models.Record.related_version_dois).raiseload('*'),
        selectinload(models.Work.records).raiseload('*'),
        selectinload(models.Work.locations).raiseload('*'),
        selectinload(models.Work.journal).raiseload('*'),
        selectinload(models.Work.references).raiseload('*'),
        selectinload(models.Work.references_unmatched).raiseload('*'),
        selectinload(models.Work.mesh),
        selectinload(models.Work.funders).selectinload
        (models.WorkFunder.funder).raiseload('*'),
        selectinload(models.Work.funders).raiseload('*'),
        selectinload(models.Work.counts_by_year).raiseload('*'),
        selectinload(models.Work.abstract),
        selectinload(models.Work.institution_assertions).raiseload('*'),
        selectinload(models.Work.institution_curation_requests).raiseload('*'),
        selectinload(models.Work.extra_ids).raiseload('*'),
        selectinload(models.Work.related_works).raiseload('*'),
        selectinload(models.Work.related_versions).raiseload('*'),
        selectinload(
            models.Work.affiliations
        ).selectinload(
            models.Affiliation.author
        ).selectinload(
            models.Author.orcids
        ).raiseload('*'),
        selectinload(
            models.Work.affiliations
        ).selectinload(
            models.Affiliation.author
        ).raiseload('*'),
        selectinload(
            models.Work.affiliations
        ).selectinload(
            models.Affiliation.institution
        ).selectinload(
            models.Institution.ror
        ).raiseload('*'),
        selectinload(
            models.Work.affiliations
        ).selectinload(
            models.Affiliation.institution
        ).raiseload('*'),
        selectinload(
            models.Work.sdg
        ).raiseload('*'),
        selectinload(
            models.Work.keywords
        ).selectinload(
            models.WorkKeyword.keyword
        ).raiseload('*'),
        selectinload(
            models.Work.concepts
        ).selectinload(
            models.WorkConcept.concept
        ).raiseload('*'),
        selectinload(
            models.Work.topics
        ).selectinload(
            models.WorkTopic.topic
        ).raiseload('*'),
        selectinload(
            models.Work.topics).selectinload(models.WorkTopic.topic
                                             ).raiseload('*'),
        selectinload(
            models.Work.topics).selectinload
        (models.WorkTopic.topic).selectinload(models.Topic.subfield
                                              ).raiseload('*'),
        selectinload(
            models.Work.topics).selectinload
        (models.WorkTopic.topic).selectinload(models.Topic.field
                                              ).raiseload('*'),
        selectinload(
            models.Work.topics).selectinload
        (models.WorkTopic.topic).selectinload(models.Topic.domain
                                              ).raiseload('*'),
        orm.Load(models.Work).raiseload('*')
    )
