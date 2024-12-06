module;

#include <fstream>

export module ik_analyzer;

import stl;
import analyze_context;
import lexeme;
import arbitrator;
import segmenter;
import analyzer;
import term;
import ik_dict;
import status;

namespace infinity {

export class IKAnalyzer : public Analyzer {
public:
    IKAnalyzer(const String &path);

    IKAnalyzer(const IKAnalyzer &other);

    ~IKAnalyzer();

    Status Load();

protected:
    int AnalyzeImpl(const Term &input, void *data, HookType func) override;

private:
    void Init();

    void LoadSegmenters();

    Lexeme *Next();

    void Reset();

    int GetLastUselessCharNum();

private:
    String dict_path_;

    bool own_dict_{};

    Dictionary *dict_{nullptr};

    UniquePtr<AnalyzeContext> context_;

    Vector<UniquePtr<Segmenter>> segmenters_;

    UniquePtr<IKArbitrator> arbitrator_;
};
} // namespace infinity