package edu.mayo.kmdp.ops.tranx.owl2;

import static java.util.Arrays.asList;
import static org.omg.spec.api4kp._20200801.AbstractCarrier.rep;
import static org.omg.spec.api4kp._20200801.AbstractCompositeCarrier.ofUniformAnonymousComposite;
import static org.omg.spec.api4kp._20200801.taxonomy.krformat.snapshot.SerializationFormat.TXT;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.snapshot.KnowledgeRepresentationLanguage.OWL_2;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.snapshot.KnowledgeRepresentationLanguage.SPARQL_1_1;
import static org.omg.spec.api4kp._20200801.taxonomy.parsinglevel._20200801.ParsingLevel.Abstract_Knowledge_Expression;

import edu.mayo.kmdp.knowledgebase.KnowledgeBaseProvider;
import edu.mayo.kmdp.knowledgebase.constructors.JenaOwlImportConstructor;
import edu.mayo.kmdp.knowledgebase.extractors.rdf.SimplePivotExtractor;
import edu.mayo.kmdp.knowledgebase.flatteners.rdf.JenaModelFlattener;
import edu.mayo.kmdp.knowledgebase.selectors.skos.JenaSKOSSelector;
import edu.mayo.kmdp.knowledgebase.selectors.sparql.v1_1.SparqlSelector;
import edu.mayo.kmdp.language.parsers.owl2.JenaOwlParser;
import edu.mayo.kmdp.language.translators.owl2.OWLtoSKOSTranscreator;
import edu.mayo.kmdp.terms.mireot.MireotExtractor;
import edu.mayo.kmdp.terms.skosifier.Owl2SkosConfig.OWLtoSKOSTxParams;
import edu.mayo.kmdp.util.PropertiesUtil;
import java.util.Properties;
import org.omg.spec.api4kp._20200801.AbstractCarrier;
import org.omg.spec.api4kp._20200801.AbstractCompositeCarrier;
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

  JenaModelFlattener jenaFlattener = new JenaModelFlattener();

  // knowledgebase
  KnowledgeBaseApiInternal kbManager = new KnowledgeBaseProvider(null)
      .withNamedSelector(SparqlSelector::new)
      .withNamedSelector(JenaSKOSSelector::new)
      .withNamedFlattener(jenaFlattener)
      .withNamedExtractor(SimplePivotExtractor::new);
  _getKnowledgeBaseStructure constructor = new JenaOwlImportConstructor(kbManager);

  KnowledgeCarrier selectQuery = AbstractCarrier.ofTree(
      MireotExtractor.MIREOT,
      rep(SPARQL_1_1, TXT));


  @Override
  public Answer<KnowledgeCarrier> applyTransrepresent(KnowledgeCarrier sourceArtifact,
      String xAccept, String xParams) {

    final Properties allprops = PropertiesUtil.parseProperties(xParams);

    Pointer kbRef = newKB();

    sourceArtifact.components()
        .map(this::parse)
        .collect(Answer.toList())
        .forEach(KnowledgeCarrier.class, owl -> addToKnowledgeBase(kbRef, owl));

    addStructureToKB(kbRef);

    return kbManager.getKnowledgeBaseComponents(kbRef.getUuid(), kbRef.getVersionTag())
        .flatList(Pointer.class, compPtr -> skosify(kbRef, compPtr, allprops))
        .map(AbstractCompositeCarrier::ofUniformAggregate);
  }


  public Answer<KnowledgeCarrier> skosify(Pointer kBaseRef, Pointer ontoPtr, Properties props) {
    Answer<Pointer> comp = extractOntologyComponent(kBaseRef, ontoPtr)
        .flatMap(this::flattenKB);

    Answer<KnowledgeCarrier> ans2 = comp
        .flatMap(kbComp -> selectSKOS(kBaseRef, kbComp, props));

    Answer<KnowledgeCarrier> ans1 = comp
        .flatMap(o -> mireotOntology(o, props))
        .flatMap(ptr -> owlToSkos(ptr, getSkosifierProperties(props, ontoPtr)));

    return ans1.flatMap(x1 ->
        ans2.flatMap(x2 -> jenaFlattener.flattenArtifact(
            ofUniformAnonymousComposite(x1.getAssetId(), asList(x1, x2)),
            x1.getAssetId().getUuid(),
            null)));
  }

  private Answer<KnowledgeCarrier> selectSKOS(
      Pointer kBaseRef, Pointer onto, Properties props) {
    return kbManager
        .namedSelect(onto.getUuid(), onto.getVersionTag(),
            JenaSKOSSelector.id, null, PropertiesUtil.serializeProps(props))
        .flatMap(ptr -> kbManager
            .getKnowledgeBaseManifestation(ptr.getUuid(), ptr.getVersionTag()));
  }


  protected Answer<KnowledgeCarrier> owlToSkos(Pointer ptr, Properties cfg) {
    return kbManager
        .getKnowledgeBaseManifestation(ptr.getUuid(), ptr.getVersionTag())
        .flatMap(kc -> skosifier.applyTransrepresent(
            kc,
            ModelMIMECoder.encode(rep(OWL_2)),
            PropertiesUtil.serializeProps(cfg)));
  }

  protected Answer<Pointer> mireotOntology(Pointer flatPtr, Properties props) {
    return kbManager
        .namedSelect(flatPtr.getUuid(), flatPtr.getVersionTag(),
            SparqlSelector.id, selectQuery, PropertiesUtil.serializeProps(props));
  }

  protected Answer<Pointer> flattenKB(Pointer pivotKB) {
    return kbManager.flatten(pivotKB.getUuid(), pivotKB.getVersionTag(), null);
  }

  protected Answer<Pointer> extractOntologyComponent(Pointer kBaseRef, Pointer ontoPtr) {
    return kbManager.extract(kBaseRef.getUuid(), kBaseRef.getVersionTag(), ontoPtr.getUuid(), null);
  }


  protected void addStructureToKB(Pointer kbRef) {
    constructor.getKnowledgeBaseStructure(kbRef.getUuid(), kbRef.getVersionTag(), null)
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


  protected Properties getSkosifierProperties(Properties props, Pointer ptr) {
    String name = ptr.getName();
    props.put(OWLtoSKOSTxParams.SCHEME_NAME.getName(), name);
    props.put(OWLtoSKOSTxParams.TOP_CONCEPT_NAME.getName(), name);
    return props;
  }


}
