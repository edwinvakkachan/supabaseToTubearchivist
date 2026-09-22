export async function delay(ms,noLog) {
  if(noLog){
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  else{
  return new Promise(resolve => setTimeout(resolve, ms));
  }
   
}
