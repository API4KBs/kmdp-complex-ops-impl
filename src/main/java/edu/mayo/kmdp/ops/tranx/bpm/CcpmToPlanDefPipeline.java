/**
 * Copyright © 2018 Mayo Clinic (RSTKNOWLEDGEMGMT@mayo.edu)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package edu.mayo.kmdp.ops.tranx.bpm;

import static java.util.Arrays.asList;
import static org.omg.spec.api4kp._20200801.AbstractCarrier.rep;
import static org.omg.spec.api4kp._20200801.AbstractCompositeCarrier.ofMixedAnonymousComposite;
import static org.omg.spec.api4kp._20200801.services.transrepresentation.ModelMIMECoder.encode;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.KnowledgeRepresentationLanguageSeries.CMMN_1_1;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.KnowledgeRepresentationLanguageSeries.FHIR_STU3;
import static org.omg.spec.api4kp._20200801.taxonomy.lexicon.LexiconSeries.PCV;
import static org.omg.spec.api4kp._20200801.taxonomy.lexicon.LexiconSeries.SNOMED_CT;
import static org.omg.spec.api4kp._20200801.taxonomy.parsinglevel.ParsingLevelSeries.Abstract_Knowledge_Expression;

import edu.mayo.kmdp.knowledgebase.KnowledgeBaseProvider;
import edu.mayo.kmdp.knowledgebase.binders.fhir.stu3.PlanDefDataShapeBinder;
import edu.mayo.kmdp.knowledgebase.flatteners.dmn.v1_2.DMN12ModelFlattener;
import edu.mayo.kmdp.knowledgebase.flatteners.fhir.stu3.PlanDefinitionFlattener;
import edu.mayo.kmdp.knowledgebase.selectors.fhir.stu3.PlanDefSelector;
import edu.mayo.kmdp.knowledgebase.weavers.fhir.stu3.PlanDefTerminologyWeaver;
import edu.mayo.kmdp.language.LanguageDeSerializer;
import edu.mayo.kmdp.language.TransrepresentationExecutor;
import edu.mayo.kmdp.language.parsers.cmmn.v1_1.CMMN11Parser;
import edu.mayo.kmdp.language.parsers.dmn.v1_2.DMN12Parser;
import edu.mayo.kmdp.language.translators.cmmn.v1_1.stu3.CmmnToPlanDefTranslator;
import edu.mayo.kmdp.language.translators.dmn.v1_2.DmnToPlanDefTranslator;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.jena.vocabulary.SKOS;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.inference.v4.server.ReasoningApiInternal._askQuery;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.CompositionalApiInternal._flattenArtifact;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal._initKnowledgeBase;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.TranscreateApiInternal._applyNamedTransform;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetCatalogApi;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetRepositoryApi;
import org.omg.spec.api4kp._20200801.api.transrepresentation.v4.server.DeserializeApiInternal;
import org.omg.spec.api4kp._20200801.api.transrepresentation.v4.server.TransxionApiInternal;
import org.omg.spec.api4kp._20200801.id.Pointer;
import org.omg.spec.api4kp._20200801.id.ResourceIdentifier;
import org.omg.spec.api4kp._20200801.services.CompositeKnowledgeCarrier;
import org.omg.spec.api4kp._20200801.services.KnowledgeCarrier;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class CcpmToPlanDefPipeline implements _applyNamedTransform, _initKnowledgeBase {

  public static final UUID id = UUID.fromString("77234f8c-718b-4429-b800-8b17369bc215");

  KnowledgeAssetCatalogApi cat;
  KnowledgeAssetRepositoryApi repo;

  URI[] annotationVocabularies;

  DeserializeApiInternal parser;

  TransxionApiInternal translator;

  _flattenArtifact flattener;

  _flattenArtifact dmnFlattener;

  KnowledgeBaseProvider kbManager;

  _askQuery dataShapeQuery;


  protected Map<Integer, Consumer<Answer<KnowledgeCarrier>>> injectors = new HashMap<>();

  public CcpmToPlanDefPipeline(
      @Autowired KnowledgeAssetCatalogApi cat,
      @Autowired KnowledgeAssetRepositoryApi repo,
      _askQuery dataShapeQuery,
      URI... annotationVocabularies
  ) {
    this.cat = cat;
    this.repo = repo;
    this.dataShapeQuery = dataShapeQuery;
    this.annotationVocabularies = annotationVocabularies;
    init();
  }

  protected void init() {
    parser =
        new LanguageDeSerializer(asList(new DMN12Parser(), new CMMN11Parser()));

    translator = new TransrepresentationExecutor(
        asList(new CmmnToPlanDefTranslator(), new DmnToPlanDefTranslator())
    );

    flattener
        = new PlanDefinitionFlattener();

    dmnFlattener
        = new DMN12ModelFlattener();

    kbManager
        = new KnowledgeBaseProvider(repo)
        .withNamedSelector(PlanDefSelector::new)
        .withNamedWeaver(PlanDefTerminologyWeaver::new)
        .withNamedBinder(PlanDefDataShapeBinder::new);

  }

  public CcpmToPlanDefPipeline addInjector(int index, Consumer<Answer<KnowledgeCarrier>> consumer) {
    injectors.put(index, consumer);
    return this;
  }

  public Consumer<Answer<KnowledgeCarrier>> injector(int j) {
    return injectors.getOrDefault(j, kc -> {
    });
  }

  @Override
  public Answer<KnowledgeCarrier> applyNamedTransform(UUID operatorId, UUID kbaseId,
      String versionTag, String xParams) {
    try {
      return doTransform(operatorId, kbaseId, versionTag, xParams);
    } finally {
      kbManager.deleteKnowledgeBase(kbaseId);
    }
  }

  protected Answer<KnowledgeCarrier> doTransform(UUID operatorId, UUID kbaseId,
      String versionTag, String xParams) {

    Answer<KnowledgeCarrier> composite =
        kbManager.getKnowledgeBaseManifestation(kbaseId, versionTag);
    UUID rootId = composite.get().mainComponent().getAssetId().getUuid();

    Answer<KnowledgeCarrier> struct = composite
        .map(CompositeKnowledgeCarrier.class::cast)
        .map(CompositeKnowledgeCarrier::getStruct);
    injector(0).accept(struct);

    // Parse
    Answer<KnowledgeCarrier> parsedComposite =
        composite.flatMap(kc -> parser.applyLift(kc, Abstract_Knowledge_Expression));
    injector(1).accept(parsedComposite);

    Answer<KnowledgeCarrier> wovenComposite = flattenDecisions(parsedComposite);

    injector(2).accept(wovenComposite);

    // Translate into PlanDefinition
    Answer<KnowledgeCarrier> planDefinitions =
        wovenComposite.flatMap(kc ->
                translator.applyTransrepresent(kc, encode(rep(FHIR_STU3, SNOMED_CT, PCV)), null));
    injector(3).accept(planDefinitions);

    // Flatten the composite, which at this point is homogeneous FHIR PlanDef
    Answer<KnowledgeCarrier> planDefinition = planDefinitions
        .reduce(kc -> flattener.flattenArtifact((CompositeKnowledgeCarrier) kc, rootId, null));
    injector(4).accept(planDefinition);

    // TODO FIXME Need to think about 'GC' for the kbManager.. when are KB released, vs overwritten?
    planDefinition.map(kc -> kbManager.deleteKnowledgeBase(kc.getAssetId().getUuid()));

    // prepare for the binding of the data shapes
    Answer<Pointer> planDefKB = planDefinition
        .flatMap(m -> kbManager.initKnowledgeBase(m, null));

    // TODO can this be simplified? The API chaining is not yet as smooth as it should be
    Answer<KnowledgeCarrier> shapedPlanDef = planDefKB
        .flatMap(pdPtr -> kbManager.namedSelect(
            pdPtr.getUuid(), pdPtr.getVersionTag(),
            PlanDefSelector.id, PlanDefSelector.pivotQuery(annotationVocabularies), null))
        .flatMap(conceptsPtr ->
            kbManager
                .getKnowledgeBaseManifestation(conceptsPtr.getUuid(), conceptsPtr.getVersionTag())
                .flatMap(selectedConcepts ->
                    dataShapeQuery.askQuery(null, null, selectedConcepts, null))
                .flatMap(bindings ->
                    planDefKB.flatMap(
                        pd -> kbManager.bind(pd.getUuid(), pd.getVersionTag(), bindings.get(0))))
                .flatMap(ptr ->
                    PlanDefTerminologyWeaver.getLexica(annotationVocabularies, cat, repo)
                        .flatMap(lex -> kbManager.namedWeave(ptr.getUuid(), ptr.getVersionTag(),
                            PlanDefTerminologyWeaver.id, lex, SKOS.altLabel.getLocalName())))
                .flatMap(ptr -> kbManager
                    .getKnowledgeBaseManifestation(ptr.getUuid(), ptr.getVersionTag())));

    injector(5).accept(shapedPlanDef);

    // And finally unwrap...
    return shapedPlanDef;
  }

  private Answer<KnowledgeCarrier> flattenDecisions(Answer<KnowledgeCarrier> parsedComposite) {
    // TODO - see if/how to improve this whole method
    CompositeKnowledgeCarrier ckc = (CompositeKnowledgeCarrier) parsedComposite.get();
    List<KnowledgeCarrier> allComps = ckc.componentList();
    List<KnowledgeCarrier> flatComps = new ArrayList<>(allComps.size());

    for (KnowledgeCarrier kc : allComps) {
      if (CMMN_1_1.sameAs(kc.getRepresentation().getLanguage())) {
        flatComps.add(kc);
      } else {
        ResourceIdentifier rootId = kc.getAssetId();
        Answer<KnowledgeCarrier> flat = dmnFlattener
            .flattenArtifact(
                ofMixedAnonymousComposite(rootId, allComps),
                rootId.getUuid(),
                null);
        if (flat.isFailure()) {
          return flat;
        } else {
          flatComps.add(flat.get());
        }
      }
    }

    ckc.components()
        .forEach(comp -> {
          KnowledgeCarrier flatComp = flatComps.stream()
              .filter(f -> comp.getAssetId().asKey().equals(f.getAssetId().asKey()))
//              .filter(f -> allComps.indexOf(comp) == flatComps.indexOf(f))
              .findFirst()
              .orElseThrow();
          comp.setExpression(flatComp.getExpression());
        });
    return Answer.of(ckc);
  }

}
