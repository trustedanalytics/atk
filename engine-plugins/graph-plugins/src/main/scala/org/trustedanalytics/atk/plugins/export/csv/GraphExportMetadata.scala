package org.trustedanalytics.atk.plugins.export.csv

import org.trustedanalytics.atk.domain.datacatalog.ExportMetadata

//TODO: the JSON serialization issue needs to be fixed such that we can return List[ExportMetadata] instead of List[String]
/**
 * Metadata class being returned by ExportGraphCsvPlugin.
 * @param metadata
 */
case class GraphExportMetadata(metadata: List[String])
