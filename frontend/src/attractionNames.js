// Bilingual names for attractions
export const attractionNames = {
  'Seven Dwarfs Mine Train': '七個小矮人礦山車',
  'Peter Pans Flight': '小飛俠天空奇遇',
  'Dumbo the Flying Elephant': '小飛象',
  'TRON Lightcycle Power Run': '創極速光輪',
  'Zootopia: Hot Pursuit': '動物方城市：熱力追蹤',
  'Soaring Over the Horizon': '翱翔·飛越地平線',
  'Rexs Racer': '抱抱龍衝天賽車',
  'Roaring Rapids': '雷鳴山漂流',
  'Jet Packs': '噴氣背包飛行器',
  'Woodys Roundup': '胡迪牛仔嘉年華',
  'Explorer Canoes': '探險家獨木舟',
  'Voyage to the Crystal Grotto': '晶彩奇航',
  'Challenge Trails at Camp Discovery': '繩索挑戰道',
  'Slinky Dog Spin': '彈簧狗團團轉',
  'Alice in Wonderland Maze': '愛麗絲夢遊仙境迷宮',
  'Once Upon a Time Adventure': '奇幻童話城堡',
  'Buzz Lightyear Planet Rescue': '巴斯光年星際營救',
  'Hunny Pot Spin': '小熊維尼歷險記',
  'TRON Lightcycle Power Run – Presented by Chevrolet': '創極速光輪－雪佛蘭呈獻'
};

export const getBilingualName = (englishName) => {
  const chineseName = attractionNames[englishName];
  return chineseName ? `${englishName}\n${chineseName}` : englishName;
};
