export function distributionBarWidth(count: number, maximum: number): string {
  if (count <= 0 || maximum <= 0) return "0%";
  return `${Math.max(8, Math.min(100, (count / maximum) * 100))}%`;
}
