package org.trustedanalytics.atk.domain.catalog

/**
 * Response class to encapsulate ls return value on a data catalog
 * @param name catalog name
 * @param metadata metadata to describe the next parameter data
 * @param data actual value of ls
 */
case class GenericCatalogResponse(override val name: String, metadata: List[String], data: List[List[String]]) extends CatalogResponse
