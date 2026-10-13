# 선행 연구·기술 비교

작성일: 2026-10-13

결론: **엣지 스탬핑의 원리는 클라우드 금융 거래소 연구(CloudEx)에 이미 있다.** 이를 먼저 밝히고, DOON이 새로 한 것으로 차별화한다.

## 1. 엣지 스탬핑 관련: "거리가 달라도 공정한 순서"

| 연구 / 기술 | 분야 | 핵심 방법 | DOON과 같은 점 | DOON과 다른 점 |
|---|---|---|---|---|
| **CloudEx** (HotOS 2021, 스탠퍼드) | 클라우드 증권거래소 | 거래소가 운영하는 게이트웨이가 동기화된 시계로 주문에 시각을 찍고, 시퀀서는 각 주문의 게이트웨이 시각 기준으로 지연 d만큼 기다린 뒤 순서대로 처리 | **원리가 거의 같다.** 게이트웨이 = PoP, 대기 d = 수집창 | 데이터센터 안(µs 거리), 등록된 참가자, 봇 문제 없음 |
| **DBO** (SIGCOMM 2023, Microsoft) | 클라우드 거래소 | 지연을 같게 만들지 않고, 참가자 간 지연 차이를 사후에 상쇄 | "지연을 없애지 않고 판정에서 뺀다"는 발상 | 시장 데이터에 대한 반응 시간 공정성, 데이터센터 규모 |
| **Jasper** (SIGCOMM 2024 포스터 / arXiv) | 클라우드 거래소 | 1,000명에게 수신 시각 차이 약 1µs로 데이터 전달 | 공정한 전달 | 보내는 쪽 공정성(DOON은 받는 쪽) |
| **Huygens** (NSDI 2018) | 시계 동기화 | 소프트웨어만으로 수십 ns 동기화 | 시계를 맞춰야 공정 판정이 가능 | 데이터센터용. DOON은 전국망에서 chrony로 µs 수준 |
| **Spanner TrueTime** (OSDI 2012, Google) | 분산 DB | 시계 오차 범위(ε)를 명시하고 그만큼 기다림(commit wait) | **오차 범위 내 동시 도착 추첨의 이론적 근거** | 정합성 목적, 공정성 목적 아님 |
| **확률적 공정 순서** (Haseeb 외, arXiv 2510.13664, 2025) | 공정 순서 이론 | 시계 오차 분포로 "먼저 일어났을 확률" 계산 | 오차 범위 안의 순서는 확정할 수 없다는 인식 | 예비 연구, 정량 평가 없음 |
| **LEQ 지연 평준화** (IEEE/ACM ToN 2012) | 네트워크 게임·화상회의 | 허브 라우터로 경로를 골라 참가자 간 지연 차이를 줄임 | 지역 간 지연 차이를 문제로 봄 | 지연 자체를 맞춤(DOON은 판정에서 뺌) |
| **Riot 인위 지연** | e스포츠 | 가까운 선수에게 일부러 지연을 넣어 균형 | 지역 격차를 공정성 문제로 봄 | 빠른 쪽을 늦춤(DOON은 아무도 늦추지 않음) |
| **IEX 350µs speed bump** | 미국 주식거래소 | 모든 주문을 350µs 늦춰 초고속 거래 이점 제거 | 속도 경쟁 자체를 불공정으로 봄 | 일괄 지연, 지리 문제 아님 |
| **Aequitas / Themis** (CRYPTO 2020 / CCS 2023) | 블록체인 합의 | 여러 노드가 본 순서를 모아 공정한 거래 순서 결정 | 순서 공정성 | 노드 간 합의 문제, 신뢰 모델 다름 |

## 2. 티켓팅 대기열: 현업 방식

| 서비스 | 방식 | 지리 격차에 대한 태도 | DOON과의 관계 |
|---|---|---|---|
| **NetFUNNEL** (STCLab, 국내 점유율 97%로 회사 주장) | 가상 대기실, 서버 도착순 순번 | 다루지 않음 | DOON 방식을 대기열 진입 순번에 적용하면 결합 가능 |
| **Queue-it 사전 대기열** | 오픈 전 도착자는 무작위 순번, 오픈 후 도착자는 선착순 | "빠른 인터넷·봇의 이점을 없애려고 랜덤"이라 명시 | 같은 문제의식. 오픈 후 도착자는 여전히 서버 도착순이라 지리 격차가 남음 |
| **Ticketmaster Verified Fan / Smart Queue** | 사전 등록자 선별 후 코드 발급 | 무작위 여부를 사장이 2026년에 부인 | 투명성 논란. DOON은 판정 근거를 기록·재현 가능 |

## 3. 매크로 방지

| 기술 | 방식 | DOON과 다른 점 |
|---|---|---|
| STCLab BotManager (예스24 적용, 트래픽 21% 차단) | 서버 앞단 SaaS 차단 | 블랙박스, 공정성 판정과 분리 |
| 마스킷 암표재판관 | 입장 단계 본인 확인 | 사후 대응 |
| FP-Inconsistent (IMC 2025), BOTracle (ESORICS 2024 워크숍) | 지문 불일치 규칙, 규칙 → ML 다단 구조 | 일반 웹 대상. 티켓팅 적용 학술 연구는 찾지 못함 |

상세 문헌은 팀 노션 "매크로 ai 문헌조사"를 참고한다.

## 4. DOON의 독창성

1. **규모와 환경:** 데이터센터 안(µs 거리, 등록 참가자)이 아니라 전국 단위 공공 연구망(KOREN)에서 익명의 일반 시민을 대상으로 한다. 게이트웨이를 사용자 가까운 지역에 두는 것 자체가 목적이다.
2. **지리적 공정성이라는 목표:** 금융은 속도 경쟁, 게임은 경기 균형이 목적이다. DOON은 "사는 곳 때문에 손해 보지 않게"가 목적이다. 이를 명시적으로 다룬 연구는 찾지 못했다.
3. **매크로 방지와의 결합:** 금융 거래소는 봇 문제가 없다. DOON은 시각을 먼저 찍고, 탐지는 자격만 판단하는 구조로 순서 공정성과 봇 방어를 한 구조에 넣었다.
4. **랜덤과 정밀 측정의 절충:** Queue-it은 공정을 위해 시각 정보를 버리고 랜덤으로 간다. DOON은 측정 가능한 만큼은 시각으로 판정하고, 측정 오차(ε) 안에서만 추첨한다(설계 보류 중). TrueTime의 ε 개념과 맞닿는다.

## 5. CloudEx에서 배울 점

- CloudEx의 대기는 **각 주문의 게이트웨이 시각 + d**를 기준으로 한다. 시각은 이른데 늦게 도착한 주문도 순서대로 끼워 넣는다.
- DOON의 창은 **중앙에 첫 요청이 도착한 순간부터 3초**다. 보정 시각이 더 빠른데 창이 닫힌 뒤 도착한 요청은 거절된다(기존 리뷰 6번 항목).
- 창 마감 기준을 **"가장 이른 보정 시각 + d"**로 바꾸면 선행 연구와 같은 방식으로 이 구멍을 막을 수 있다. "CloudEx와 비교해 이 부분을 이렇게 고쳤다"고 말할 수 있는 소재다.

## 6. 발표용 문장 (제안)

> 엣지에서 시각을 찍는 원리는 클라우드 금융 거래소(CloudEx, 2021)에서 검증되었습니다. 저희는 이 원리를 처음으로 **국가 연구망 전국 PoP 위에서 일반 시민의 선착순 서비스**에 적용하고, **매크로 방지를 같은 판정 구조 안에 결합**했습니다.

## 확인 수준

- CloudEx: PDF 원문으로 구조 확인(게이트웨이 타임스탬프, 시퀀서 지연 d_s, hold/release 버퍼).
- DBO, Jasper, Huygens, LEQ, Aequitas/Themis, 확률적 공정 순서: 초록·공식 페이지 수준 확인.
- Spanner TrueTime: 강의자료·2차 자료 수준. 발표 인용 전 원문 확인 필요.

## 출처

- [CloudEx (HotOS 2021)](https://www.sigops.org/s/conferences/hotos/2021/papers/hotos21-s06-ghalayini.pdf)
- [DBO (SIGCOMM 2023, arXiv 2303.16139)](https://arxiv.org/pdf/2303.16139), [Microsoft 페이지](https://www.microsoft.com/en-us/research/publication/dbo-fairness-for-cloud-hosted-financial-exchanges/)
- [Jasper (arXiv 2402.09527)](https://arxiv.org/pdf/2402.09527)
- [Huygens (NSDI 2018)](https://www.usenix.org/conference/nsdi18/presentation/geng)
- [Spanner 강의자료 (Princeton COS418)](https://www.cs.princeton.edu/courses/archive/spr24/cos418/docs/L17-spanner-pt1.pdf), [InfoQ](https://www.infoq.com/news/2012/10/google-spanner)
- [확률적 공정 순서 (arXiv 2510.13664)](https://arxiv.org/pdf/2510.13664)
- [LEQ (IEEE/ACM ToN 2012)](https://minlanyu.seas.harvard.edu/writeup/ton12.pdf)
- [Riot 인위 지연](https://lolesports.com/en-US/news/riot-games-tech-blog-artificial-latency-for-remote-competitors)
- [IEX speed bump](https://thehedgefundjournal.com/the-sec-approves-the-investors-exchange-speed-bump/)
- [Aequitas (ePrint 2020/269)](https://eprint.iacr.org/2020/269), [Themis (ePrint 2021/1465)](https://eprint.iacr.org/2021/1465)
- [NetFUNNEL](https://docs.stclab.com/products/netfunnel/intro), [Queue-it 사전 대기열](https://queue-it.com/pre-queue/), [Queue-it 랜덤화 설명](https://www.queue-it.com/blog/first-in-first-out-randomization), [Newsweek (Ticketmaster 랜덤 논란)](https://www.newsweek.com/ticketmaster-said-queue-system-random-president-isnt-sure-11954673)
- [스포츠경향 (예스24 봇 차단)](https://sports.khan.co.kr/article/202505290458003/amp), [스포츠경향 (마스킷)](https://sports.khan.co.kr/article/202605281529003)
