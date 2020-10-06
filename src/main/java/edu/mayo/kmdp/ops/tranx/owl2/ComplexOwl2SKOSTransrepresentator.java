package edu.mayo.kmdp.ops.tranx.owl2;

import static org.omg.spec.api4kp._20200801.AbstractCarrier.rep;
import static org.omg.spec.api4kp._20200801.taxonomy.krformat.snapshot.SerializationFormat.TXT;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.snapshot.KnowledgeRepresentationLanguage.OWL_2;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.snapshot.KnowledgeRepresentationLanguage.SPARQL_1_1;
import static org.omg.spec.api4kp._20200801.taxonomy.parsinglevel._20200801.ParsingLevel.Abstract_Knowledge_Expression;

import edu.mayo.kmdp.knowledgebase.KnowledgeBaseProvider;
import edu.mayo.kmdp.knowledgebase.constructors.JenaOwlImportConstructor;
import edu.mayo.kmdp.knowledgebase.extractors.rdf.SimplePivotExtractor;
import edu.mayo.kmdp.knowledgebase.flatteners.rdf.JenaModelFlattener;
import edu.mayo.kmdp.knowledgebase.selectors.sparql.v1_1.SparqlSelector;
import edu.mayo.kmdp.language.parsers.owl2.JenaOwlParser;
import edu.mayo.kmdp.language.translators.owl2.OWLtoSKOSTranscreator;
import edu.mayo.kmdp.terms.mireot.MireotConfig;
import edu.mayo.kmdp.terms.mireot.MireotConfig.MireotParameters;
import edu.mayo.kmdp.terms.mireot.MireotExtractor;
import edu.mayo.kmdp.terms.skosifier.Owl2SkosConfig;
import edu.mayo.kmdp.terms.skosifier.Owl2SkosConfig.OWLtoSKOSTxParams;
import org.omg.spec.api4kp._20200801.AbstractCarrier;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal._getKnowledgeBaseStructure;
import org.omg.spec.api4kp._20200801.api.transrepresentation.v4.server.DeserializeApiInternal._applyLift;
import org.omg.spec.api4kp._20200801.api.transrepresentation.v4.server.TransxionApiInternal._applyTransrepresent;
import org.omg.spec.api4kp._20200801.id.Pointer;
import org.omg.spec.api4kp._20200801.services.KnowledgeCarrier;
import org.omg.spec.api4kp._20200801.services.transrepresentation.ModelMIMECoder;

public class ComplexOwl2SKOSTransrepresentator implements _applyTransrepresent {

  // parser
  _applyLift parser = new JenaOwlParser();

  // tranx
  _applyTransrepresent skosifier = new OWLtoSKOSTranscreator();

  // knowledgebase
  KnowledgeBaseApiInternal kbManager = new KnowledgeBaseProvider(null)
      .withNamedSelector(SparqlSelector::new)
      .withNamedFlattener(new JenaModelFlattener())
      .withNamedExtractor(SimplePivotExtractor::new);
  _getKnowledgeBaseStructure constructor = new JenaOwlImportConstructor(kbManager);

  KnowledgeCarrier selectQuery = AbstractCarrier.ofTree(
      MireotExtractor.MIREOT,
      rep(SPARQL_1_1, TXT));


  @Override
  public Answer<KnowledgeCarrier> applyTransrepresent(KnowledgeCarrier sourceArtifact,
      String xAccept, String xParams) {

    Pointer kbRef = newKB();

    sourceArtifact.components()
        .map(this::parse)
        .collect(Answer.toList())
        .forEach(KnowledgeCarrier.class, owl -> addToKnowledgeBase(kbRef, owl));

    addStructureToKB(kbRef);

    return kbManager.getKnowledgeBaseComponents(kbRef.getUuid(), kbRef.getVersionTag())
        .flatList(Pointer.class, compPtr -> skosify(kbRef, compPtr))
        .map(AbstractCarrier::ofHeterogeneousComposite);
  }


  public Answer<KnowledgeCarrier> skosify(Pointer kBaseRef, Pointer ontoPtr) {
    return extractOntologyComponent(kBaseRef,ontoPtr)
        .flatMap(this::flattenKB)
        .flatMap(this::mireotOntology)
        .flatMap(kc -> owlToSkos(kc, getSkosifierProperties(ontoPtr).encode()));
  }

  protected Answer<KnowledgeCarrier> owlToSkos(KnowledgeCarrier kc, String cfg) {
    return skosifier.applyTransrepresent(
            kc,
            ModelMIMECoder.encode(rep(OWL_2)),
            cfg);
  }

  protected Answer<KnowledgeCarrier> mireotOntology(Pointer flatPtr) {
    return kbManager
        .select(flatPtr.getUuid(), flatPtr.getVersionTag(), selectQuery, getMireotProperties().encode())
        .flatMap(ptr -> kbManager
            .getKnowledgeBaseManifestation(ptr.getUuid(), ptr.getVersionTag()));
  }

  protected Answer<Pointer> flattenKB(Pointer pivotKB) {
    return kbManager.flatten(pivotKB.getUuid(), pivotKB.getVersionTag(), "clear");
  }

  protected Answer<Pointer> extractOntologyComponent(Pointer kBaseRef, Pointer ontoPtr) {
    return kbManager.extract(kBaseRef.getUuid(), kBaseRef.getVersionTag(), ontoPtr.getUuid(), null);
  }


  protected void addStructureToKB(Pointer kbRef) {
    constructor.getKnowledgeBaseStructure(kbRef.getUuid(), kbRef.getVersionTag())
        .flatMap(struct -> kbManager
            .setKnowledgeBaseStructure(kbRef.getUuid(), kbRef.getVersionTag(), struct));
  }

  protected Pointer newKB() {
    return kbManager.initKnowledgeBase().orElseThrow(IllegalStateException::new);
  }

  protected Answer<KnowledgeCarrier> parse(KnowledgeCarrier binaryOntology) {
    return parser.applyLift(binaryOntology,
        Abstract_Knowledge_Expression,
        ModelMIMECoder.encode(rep(OWL_2)), null);
  }

  protected void addToKnowledgeBase(Pointer kbRef, KnowledgeCarrier parsedOntology) {
    kbManager.populateKnowledgeBase(kbRef.getUuid(), kbRef.getVersionTag(), parsedOntology);
  }

  protected MireotConfig getMireotProperties() {
    return new MireotConfig()
        .with(MireotParameters.BASE_URI,
            "http://ontology.mayo.edu/ontologies/clinicalsituationontology/")
        .with(MireotParameters.MIN_DEPTH, "1")
        .with(MireotParameters.TARGET_URI,
            "http://ontology.mayo.edu/ontologies/clinicalsituationontology/3c338081-b709-427e-8a51-76bb8f6ef26d");
  }

  protected Owl2SkosConfig getSkosifierProperties(Pointer kc) {
    String name = kc.getName();
    return new Owl2SkosConfig()
        .with(OWLtoSKOSTxParams.SCHEME_NAME, name + "ClinicalSituations")
        .with(OWLtoSKOSTxParams.TOP_CONCEPT_NAME, name + "ClinicalSituation")
        .with(OWLtoSKOSTxParams.TGT_NAMESPACE,
            "https://ontology.mayo.edu/taxonomies/clinicalsituations");
  }


}
