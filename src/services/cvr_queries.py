import json

def cvr_virk_query(size: int) -> dict:

    if not size:
        size = 10

    query = json.dumps(
        {
        "_source": 
        [
            "Vrvirksomhed.sidstOpdateret",
            "Vrvirksomhed.navne",
            "Vrvirksomhed.beliggenhedsadresse",
            "Vrvirksomhed.cvrNummer",
            "Vrvirksomhed.penheder",
            "Vrvirksomhed.virksomhedMetadata.nyesteNavn.navn",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.kommune.kommuneNavn",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.kommune.kommuneKode",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.kommune.periode",
            "Vrvirksomhed.virksomhedMetadata.nyesteHovedbranche.branchekode",
            "Vrvirksomhed.virksomhedMetadata.nyesteHovedbranche.branchetekst",
            "Vrvirksomhed.virksomhedMetadata.nyesteHovedbranche.periode",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.vejnavn",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.husnummerFra",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.husnummerTil",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.bogstavFra",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.bogstavTil",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.etage",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.sidedoer",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.conavn",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.postnummer",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.postdistrikt",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.landekode",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.vejkode",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.periode",
            "Vrvirksomhed.virksomhedMetadata.nyesteVirksomhedsform.virksomhedsformkode",
            "Vrvirksomhed.virksomhedMetadata.nyesteVirksomhedsform.langBeskrivelse",
            "Vrvirksomhed.virksomhedMetadata.nyesteVirksomhedsform.ansvarligDataleverandoer",
            "Vrvirksomhed.virksomhedMetadata.nyesteVirksomhedsform.periode",
            "Vrvirksomhed.hjemmeside",
            "Vrvirksomhed.telefonNummer",
            "Vrvirksomhed.elektroniskPost",
            "Vrvirksomhed.virksomhedMetadata.nyesteKontaktoplysninger",
            "Vrvirksomhed.virksomhedMetadata.sammensatStatus",
            "Vrvirksomhed.virksomhedMetadata.nyesteErstMaanedsbeskaeftigelse.antalAnsatte",
            "Vrvirksomhed.maanedsbeskaeftigelse",
            "Vrvirksomhed.erstMaanedsbeskaeftigelse"
        ],
        "query": {
            "bool": {
                "must_not": [
                    {
                    "exists": {
                        "field": "Vrvirksomhed.livsforloeb.periode.gyldigTil"
                        }
                    }
                ]
            }
        },
        "size": size
        })

    return query
