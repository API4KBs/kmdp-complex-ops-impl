package edu.mayo.kmdp.ops.select.fhir.stu3;

import static java.util.stream.Collectors.toList;
import static org.omg.spec.api4kp._20200801.AbstractCarrier.rep;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.KnowledgeRepresentationLanguageSeries.FHIR_STU3;

import edu.mayo.kmdp.knowledgebase.KnowledgeBaseProvider;
import edu.mayo.kmdp.knowledgebase.selectors.fhir.stu3.FHIRQuestionnaireSelector;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import org.hl7.fhir.dstu3.model.Questionnaire;
import org.hl7.fhir.dstu3.model.ValueSet;
import org.omg.spec.api4kp._20200801.AbstractCarrier;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal._select;
import org.omg.spec.api4kp._20200801.id.Pointer;
import org.omg.spec.api4kp._20200801.services.KnowledgeCarrier;

public class MultiQuestionnaireConceptSelector implements _select {

  KnowledgeBaseApiInternal kbManager;

  @Inject
  public MultiQuestionnaireConceptSelector(KnowledgeBaseApiInternal kbManager) {
    // This KBManager needs a FHIRQuestionnaireSelector
    // The case will not be necessary once the proper discovery/registration APIs are implemented
    KnowledgeBaseProvider kbp = (KnowledgeBaseProvider) kbManager;
    if (!kbp.hasNamedComponent(FHIRQuestionnaireSelector.id)) {
      kbp.withNamedSelector(FHIRQuestionnaireSelector::new);
    }
    this.kbManager = kbManager;
  }

  @Override
  public Answer<Pointer> select(UUID kbaseId, String versionTag, KnowledgeCarrier selectDefinition,
      String xParams) {

    return kbManager.getKnowledgeBaseManifestation(kbaseId, versionTag)
        .map(kb -> kb.componentsAs(Questionnaire.class).collect(toList()))
        .flatList(Questionnaire.class, q -> selectConcepts(q, selectDefinition))
        // TODO FIXME define 'flatStream'
        .map(List::stream)
        .reduce(ValueSet.class, this::merge)
        .map(vs -> AbstractCarrier.ofAst(vs, rep(FHIR_STU3)))
        .flatMap(m -> kbManager.initKnowledgeBase(m, null));
  }

  private ValueSet merge(ValueSet v1, ValueSet v2) {
    ValueSet v3 = new ValueSet();
    v3.getExpansion().getContains().addAll(v1.getExpansion().getContains());
    v3.getExpansion().getContains().addAll(v2.getExpansion().getContains());
    return v3;
  }

  private Answer<ValueSet> selectConcepts(Questionnaire questionnaire, KnowledgeCarrier selectDef) {
    return kbManager
        .initKnowledgeBase(AbstractCarrier.ofAst(questionnaire, rep(FHIR_STU3)), null)
        .flatMap(
            ptr -> kbManager.select(
                ptr.getUuid(),
                ptr.getVersionTag(),
                selectDef))
        .flatMap(
            ptr -> kbManager.getKnowledgeBaseManifestation(ptr.getUuid(), ptr.getVersionTag()))
        .map(kc -> kc.as(ValueSet.class).orElseThrow());
  }


}
