package edu.mayo.kmdp.ops.select.bpm;

import static edu.mayo.kmdp.util.JenaUtil.datA;
import static edu.mayo.kmdp.util.JenaUtil.objA;
import static org.omg.spec.api4kp._20200801.AbstractCarrier.ofAst;
import static org.omg.spec.api4kp._20200801.AbstractCarrier.rep;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.KnowledgeRepresentationLanguageSeries.OWL_2;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.KnowledgeRepresentationLanguageSeries.SPARQL_1_1;

import edu.mayo.kmdp.kbase.query.sparql.v1_1.JenaQuery;
import edu.mayo.kmdp.knowledgebase.KnowledgeBaseProvider;
import edu.mayo.kmdp.knowledgebase.constructors.dmn.v1_2.DMN12ImportConstructor;
import edu.mayo.kmdp.knowledgebase.extractors.rdf.SimplePivotExtractor;
import edu.mayo.kmdp.knowledgebase.flatteners.dmn.v1_2.DMN12ModelFlattener;
import edu.mayo.kmdp.knowledgebase.selectors.dmn.v1_2.DMN12ConceptSelector;
import edu.mayo.kmdp.language.parsers.dmn.v1_2.DMN12Parser;
import edu.mayo.kmdp.util.PropertiesUtil;
import java.util.List;
import java.util.UUID;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.omg.spec.api4kp._20200801.AbstractCarrier;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.KnowledgeBaseApi;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal._select;
import org.omg.spec.api4kp._20200801.datatypes.Bindings;
import org.omg.spec.api4kp._20200801.id.Pointer;
import org.omg.spec.api4kp._20200801.services.KnowledgeCarrier;

public class CPMConceptSelector implements _select {

  // Language
  DMN12Parser parser;

  // KB
  KnowledgeBaseApi kbManager;
  DMN12ImportConstructor constructor;

  // Reasoning
  JenaQuery qry;

  public CPMConceptSelector(KnowledgeBaseProvider kbase) {
    init(kbase);
  }

  private void init(KnowledgeBaseProvider kbase) {
    // Language
    parser = new DMN12Parser();
    // KB
    kbManager = KnowledgeBaseApi.newInstance(
        kbase
            .withNamedSelector(DMN12ConceptSelector::new)
            .withNamedExtractor(SimplePivotExtractor::new)
            .withNamedFlattener(DMN12ModelFlattener::new));
    constructor = new DMN12ImportConstructor(kbManager);
    // Reasoning
    qry = new JenaQuery(kbManager);
  }


  @Override
  public Answer<Pointer> select(UUID kbaseId, String versionTag, KnowledgeCarrier selectDefinition,
      String xParams) {

    constructor.getKnowledgeBaseStructure(kbaseId, versionTag, xParams)
        .flatMap(struct -> kbManager
            .setKnowledgeBaseStructure(kbaseId, versionTag, struct));

    Answer<Model> model = kbManager.getKnowledgeBaseComponents(kbaseId, versionTag, xParams)
        .flatList(Pointer.class,
            ptr -> getConceptsForComponent(kbaseId, versionTag,
                ptr, selectDefinition, xParams))
        .flatOpt(list -> ((List<Model>) list).stream().reduce(Model::add));

    return model
        .map(m -> ofAst(m, rep(OWL_2)))
        .flatMap(m -> kbManager.initKnowledgeBase(m, xParams));
  }

  private Answer<Model> getConceptsForComponent(UUID kbaseId, String versionTag,
      Pointer componentId, KnowledgeCarrier selectDefinition, String xParams) {
    boolean excluded = PropertiesUtil.parse(xParams)
        .flatMap(props -> PropertiesUtil.pString("exclude", props))
        .map(excl -> componentId.getUuid().toString().equals(excl))
        .orElse(false);
    if (excluded) {
      return Answer.of(ModelFactory.createDefaultModel());
    }
    return kbManager
        // focus on the specific component (model)
        .extract(kbaseId, versionTag, componentId.getUuid(), null)
        // resolve all imports
        .flatMap(ptr -> kbManager.flatten(ptr.getUuid(), ptr.getVersionTag()))
        // select all the annotations, returning the concepts as a SKOS A-box
        .flatMap(ptr -> kbManager.select(ptr.getUuid(), ptr.getVersionTag(), selectDefinition))
        // query for the concept id/label
        .flatMap(ptr -> qry.askQuery(ptr.getUuid(), ptr.getVersionTag(),
            AbstractCarrier.of(
                "SELECT ?c ?l "
                    + "WHERE { ?c <" + RDFS.label + "> ?l. }",
                rep(SPARQL_1_1)),
            null))
        .mapList(Bindings.class, this::formulateConcept)
        .flatOpt(list -> ((List<Model>)list).stream().reduce(Model::add));
  }


  private Model formulateConcept(Bindings<String, Object> bind) {
    Model m = ModelFactory.createDefaultModel();
    String c = bind.get("c").toString();
    m.add(objA(c, RDF.type.getURI(), SKOS.Concept.getURI()));
    m.add(datA(c, RDFS.label.getURI(), bind.get("l").toString()));
    return m;
  }

}
