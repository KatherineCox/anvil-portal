/*
 * The AnVIL
 * https://www.anvilproject.org
 *
 * Service for dashboard workspaces data ingestion.
 */

// Core dependencies
const fs = require("fs");
const path = require("path");

// App dependencies
const {sortDataByDuoTypes} = require(path.resolve(__dirname, "./dashboard-sort.service.js"));
const {getStudyGapAccession} = require(path.resolve(__dirname, "./dashboard-xml.service.js"));

// Template variables
const fileAnVILDataIngestion = "anvil-data-ingestion-attributes.tsv";
const fileAnVILDataIngestionAccession = "anvil-data-ingestion-attributes-accession.tsv";
const fileTerraDataIngestionCounts = "terra-data-ingestion-attributes-counts.tsv";
const fileTerraDataIngestionFileSize = "terra-data-ingestion-attributes-file-size.tsv";

const ALLOW_LIST_WORKSPACE_FIELD_ARRAY = ["consentShortNames", "dataTypes", "diseases"];
const ALLOW_LIST_WORKSPACE_FIELD_NUMBER = ["size", "samples", "subjects"];
const ALLOW_LIST_WORKSPACE_ACCESS_PUBLIC = ["1000G-high-coverage-2019"];
const WORKSPACE_CONSORTIUM_DISPLAY_VALUE = {
    "CCDG": "CCDG",
    "CMG": "CMG",
    "EMERGE": "eMERGE",
    "GTEX": "GTEx (v8)",
    "NHGRI": "NHGRI",
    "PAGE": "PAGE",
    "THOUSANDGENOMES": "1000 Genomes"
};
const INGESTION_HEADERS_TO_WORKSPACE_KEY = {
    "CONSENT_SHORT_NAMES": "library:dataUseRestriction",
    "DATA_TYPES": "library:datatype.items",
    "DB_GAP_ID": "study_accession",
    "DISEASES": "library:indication",
    "PROJECT_ID": "name",
    "LIBRARY_PROJECT_NAME": "library:projectName",
    "SAMPLES": "Sample Count",
    "SIZE": "File Size",
    "SUBJECTS": "library:numSubjects",
    "WORKSPACE": "Workspace"
};
const HEADERS_TO_WORKSPACE_KEY = {
    "ACCESS": "access",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.CONSENT_SHORT_NAMES]: "consentShortNames",
    "CONSORTIUM": "consortium",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.DATA_TYPES]: "dataTypes",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.DB_GAP_ID]: "dbGapId",
    "DB_GAP_ID_ACCESSION": "dbGapIdAccession",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.DISEASES]: "diseases",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.PROJECT_ID]: "projectId",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.SAMPLES]: "samples",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.SIZE]: "size",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.SUBJECTS]: "subjects",
    [INGESTION_HEADERS_TO_WORKSPACE_KEY.WORKSPACE]: "projectId"
};

/**
 * Returns the workspaces ingested data.
 *
 * @returns {Promise.<void>}
 */
const getIngestedWorkspaces = async function getIngestedWorkspaces() {

    /* Build map object key-value pair of study id by workspace project id. */
    const studyIdByProjectId = await getStudyIdByProjectId();

    /* Build map object key-value pair of sample count by workspace project id. */
    const sampleCountByProjectId = await getSampleCountByProjectId();

    /* Build map object key-value pair of file size by workspace project id. */
    const fileSizeByProjectId = await getFileSizeByProjectId();

    /* Build the workspaces from ingested data. */
    const workspaces = await getWorkspaces(sampleCountByProjectId, studyIdByProjectId, fileSizeByProjectId);

    /* Return the sorted dashboard. */
    return sortDataByDuoTypes(workspaces, HEADERS_TO_WORKSPACE_KEY.CONSORTIUM, HEADERS_TO_WORKSPACE_KEY[INGESTION_HEADERS_TO_WORKSPACE_KEY.PROJECT_ID]);
};

/**
 * Returns map object key-value pair from ingested data, specified by key and value.
 *
 * @param contentRows
 * @param headers
 * @param keyPair
 * @param valuePair
 */
function buildKeyValuePair(contentRows, headers, keyPair, valuePair) {

    /* Grab from each content row the ingested data, using the headers as the key. */
    return contentRows
        .slice(1)
        .reduce((acc, contentRow) => {

            const row = contentRow
                .split("\t")
                .reduce((acc, datum, i) => {

                    const header = headers[i];

                    if ( isHeaderKeyOrValue(header, keyPair, valuePair) ) {

                        const [key, value] = getIngestedDatumKeyValuePair(datum, header);

                        acc = Object.assign(acc, {[key]: value})
                    }

                    return acc;
                }, {});

            const keyKeyPair = HEADERS_TO_WORKSPACE_KEY[INGESTION_HEADERS_TO_WORKSPACE_KEY[keyPair]];
            const keyValuePair = HEADERS_TO_WORKSPACE_KEY[INGESTION_HEADERS_TO_WORKSPACE_KEY[valuePair]];

            acc.set(row[keyKeyPair], row[keyValuePair]);

            return acc;
        }, new Map());
}

/**
 * Returns the ingested headers.
 *
 * @param contentRows
 * @returns {Array}
 */
function buildIngestedHeaders(contentRows) {

    return contentRows
        .slice(0, 1)
        .toString()
        .split("\t")
}

/**
 * Returns the ingested datum, corrected for type.
 * i.e. will return a number as Number, instead of a string.
 *
 * @param datum
 * @param key
 * @returns {*}
 */
function buildIngestedDatum(datum, key) {

    const value = formatIngestedDatum(datum, key);

    if ( ALLOW_LIST_WORKSPACE_FIELD_ARRAY.includes(key) ) {

        return value.split(",");
    }

    if ( ALLOW_LIST_WORKSPACE_FIELD_NUMBER.includes(key) ) {

        return Number(value.replace(/,/g, ""));
    }

    return value;
}

/**
 * Returns the ingested workspace.
 *
 * @param contentRow
 * @param headers
 * @param sampleCountByProjectId
 * @param studyIdByProjectId
 * @param fileSizeByProjectId
 * @returns {*}
 */
async function buildWorkspaceRow(contentRow, headers, sampleCountByProjectId, studyIdByProjectId, fileSizeByProjectId) {

    /* Grab the ingested row data. */
    const row = contentRow
        .split("\t")
        .reduce((acc, datum, i) => {

            const header = headers[i];
            const [key, value] = getIngestedDatumKeyValuePair(datum, header);

            /* Only include data we are interested in. */
            if ( key ) {

                acc = Object.assign(acc, {[key]: value});
            }

            return acc;
        }, {});

    /* Define additional workspace property keys. */
    const keyAccess = HEADERS_TO_WORKSPACE_KEY.ACCESS;
    const keyConsortium = HEADERS_TO_WORKSPACE_KEY.CONSORTIUM;
    const keyProjectId = HEADERS_TO_WORKSPACE_KEY[INGESTION_HEADERS_TO_WORKSPACE_KEY.PROJECT_ID];
    const keySamplesCount = HEADERS_TO_WORKSPACE_KEY[INGESTION_HEADERS_TO_WORKSPACE_KEY.SAMPLES];
    const keySize = HEADERS_TO_WORKSPACE_KEY[INGESTION_HEADERS_TO_WORKSPACE_KEY.SIZE];
    const keyStudyAccession = HEADERS_TO_WORKSPACE_KEY.DB_GAP_ID_ACCESSION;
    const keyStudyId = HEADERS_TO_WORKSPACE_KEY[INGESTION_HEADERS_TO_WORKSPACE_KEY.DB_GAP_ID];

    /* Grab the project id. */
    const projectId = row[keyProjectId];

    /* Grab the workspace's consortium, samples count and study id. */
    const access = ALLOW_LIST_WORKSPACE_ACCESS_PUBLIC.includes(projectId) ? "Public" : "Private";
    const [,consortium,] = projectId.split("_");
    const consortiumDisplayValue = buildIngestedDatum(consortium, HEADERS_TO_WORKSPACE_KEY.CONSORTIUM);
    const samplesCount = sampleCountByProjectId.get(projectId) || 0;
    const size = fileSizeByProjectId.get(projectId) || 0;
    const studyId = studyIdByProjectId.get(projectId);
    const studyAccession = await getStudyGapAccession(studyId);

    /* Build additional workspace properties. */
    const workspace = {
        [keyAccess]: access,
        [keyConsortium]: consortiumDisplayValue,
        [keySamplesCount]: samplesCount,
        [keySize]: size,
        [keyStudyAccession]: studyAccession,
        [keyStudyId]: studyId};

    return Object.assign(row, workspace);
}

/**
 * Returns the ingested workspaces.
 *
 * @param contentRows
 * @param headers
 * @param sampleCountByProjectId
 * @param studyIdByProjectId
 * @param fileSizeByProjectId
 */
async function buildWorkspaces(contentRows, headers, sampleCountByProjectId, studyIdByProjectId, fileSizeByProjectId) {

    /* Grab from each content row the ingested data. */
    return await Promise.all(contentRows
        .slice(1)
        .map(contentRow => buildWorkspaceRow(contentRow, headers, sampleCountByProjectId, studyIdByProjectId, fileSizeByProjectId)));
}

/**
 * Returns formatted ingested datum, specified by key.
 *
 * @param datum
 * @param key
 * @returns {*}
 */
function formatIngestedDatum(datum, key) {

    if ( key === HEADERS_TO_WORKSPACE_KEY.CONSORTIUM ) {

        const consortium = datum.toUpperCase();

        return WORKSPACE_CONSORTIUM_DISPLAY_VALUE[consortium] || consortium;
    }

    return datum;
}

/**
 * Returns the contents of the specified file, as an array.
 * Each element of the array represents a row (as a string value) from the file.
 *
 * @param fileName
 * @returns {Promise.<Array>}
 */
async function getFileContents(fileName) {

    /* Only return ingested workspaces if the file exists. */
    if ( fs.existsSync(path.resolve(__dirname, fileName)) ) {

        const filePath = path.resolve(__dirname, fileName);
        const fileContent = await fs.readFileSync(filePath, "utf8");

        /* Return the file content as an array. */
        return fileContent.toString().split("\r\n");
    }
    else {

        /* File does not exist. */
        console.log(`Error: file ${fileName} cannot be found.`);
        return [];
    }
}

async function getFileSizeByProjectId() {

    /* Grab the header and file contents. */
    const [headers, contentRows] = await getIngestedData(fileTerraDataIngestionFileSize);

    /* Return map object key value pair file size by project id. */
    return buildKeyValuePair(contentRows, headers, "WORKSPACE", "SIZE");
}

/**
 * Returns the ingested header and content for the specified file.
 *
 * @param fileName
 * @returns {Promise.<[null,null]>}
 */
async function getIngestedData(fileName) {

    /* Grab the file contents. */
    const contentRows = await getFileContents(fileName);

    /* Grab the data header row. */
    const headers = buildIngestedHeaders(contentRows);

    return [headers, contentRows];
}

/**
 * Returns the datum key (if it exists), and corresponding formatted value.
 *
 * @param datum
 * @param header
 * @returns {[null,null]}
 */
function getIngestedDatumKeyValuePair(datum, header) {

    const key = HEADERS_TO_WORKSPACE_KEY[header];
    const value = buildIngestedDatum(datum, key);

    return [key, value];
}

/**
 * Returns map object key-value pair of sample count by project id.
 *
 * @returns {Promise.<void>}
 */
async function getSampleCountByProjectId() {

    /* Grab the header and file contents. */
    const [headers, contentRows] = await getIngestedData(fileTerraDataIngestionCounts);

    /* Return map object key value pair sample count by project id. */
    return buildKeyValuePair(contentRows, headers, "WORKSPACE", "SAMPLES");
}

/**
 * Returns map object key-value pair of study id by project id.
 *
 * @returns {Promise.<*>}
 */
async function getStudyIdByProjectId() {

    /* Grab the header and file contents. */
    const [headers, contentRows] = await getIngestedData(fileAnVILDataIngestionAccession);

    /* Return map object key value pair study id by project id. */
    return buildKeyValuePair(contentRows, headers, "PROJECT_ID", "DB_GAP_ID");
}

/**
 * Returns ingested workspaces data.
 *
 * @param sampleCountByProjectId
 * @param studyIdByProjectId
 * @param fileSizeByProjectId
 * @returns {Promise.<*>}
 */
async function getWorkspaces(sampleCountByProjectId, studyIdByProjectId, fileSizeByProjectId) {

    /* Grab the header and file contents. */
    const [headers, contentRows] = await getIngestedData(fileAnVILDataIngestion);

    /* Return all ingested workspaces. */
    return await buildWorkspaces(contentRows, headers, sampleCountByProjectId, studyIdByProjectId, fileSizeByProjectId);
}

/**
 * Returns true if the header equals the specified key or value pair.
 *
 * @param header
 * @param keyPair
 * @param valuePair
 * @returns {boolean}
 */
function isHeaderKeyOrValue(header, keyPair, valuePair) {

    const keyExists = header === INGESTION_HEADERS_TO_WORKSPACE_KEY[keyPair];
    const valueExists = header === INGESTION_HEADERS_TO_WORKSPACE_KEY[valuePair];

    return  keyExists || valueExists;
}

module.exports.getIngestedWorkspaces = getIngestedWorkspaces;
